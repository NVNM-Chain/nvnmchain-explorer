//! Decoded logs, in English.
//!
//! A log arrives as `Transfer(0xab…, 0xcd…, 1500000)`; [`known_events`] turns
//! it into "Send 1.5 pathUSD to 0xcd12…3456". [`build_summary`] then picks
//! which of a receipt's sentences heads the page, or says why there is none:
//! "Transfer failed: insufficient pathUSD balance".
//!
//! Phrasings live in `PHRASES`, keyed by canonical event signature so that a
//! TIP-20 `Burn` and the fee AMM's never share a sentence. Events that read
//! differently depending on an argument — a mint to someone else, a transfer
//! that is really a fee — are adjusted in `refine` afterwards.

use std::collections::HashMap;

use serde::Serialize;

use crate::decoder::{
    checksum_address, decode_revert, is_valid_address, keccak_hex, revert_data_in, truncate_hash,
    DecodedError, DecodedEvent, DecodedParam, FEE_MANAGER_ADDRESS,
};
use crate::memo::{self, Memo};
use crate::tempo_address::is_virtual;
use crate::tokens::format_token_amount;

/// `0x1234…5678` — long enough to recognise, short enough to read in a line.
fn truncate(value: &str) -> String {
    truncate_hash(value, 4, 4)
}

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// What the summary needs to know about a token to spell an amount.
#[derive(Debug, Clone, Default)]
pub struct TokenDisplay {
    pub symbol: String,
    pub decimals: i64,
}

/// Token metadata by lowercase address. Lowercase because the address in a log
/// and the address in the database need not agree on checksum casing.
pub type Tokens = HashMap<String, TokenDisplay>;

// ---------------------------------------------------------------------------
// Phrase table
// ---------------------------------------------------------------------------

/// One piece of a sentence.
#[derive(Debug, Clone, Copy)]
enum Slot {
    /// A literal connective — "to", "for", "from".
    Word(&'static str),
    /// A quantity of the token that emitted the log (a TIP-20's own events).
    Amount(&'static str),
    /// A quantity of the token named by another parameter.
    AmountOf {
        value: &'static str,
        token: &'static str,
    },
    /// An address, truncated.
    Account(&'static str),
    /// A token address, shown by symbol when one is known.
    Token(&'static str),
    /// Any parameter, as decoded.
    Value(&'static str),
    /// A hash or identifier, truncated.
    Hex(&'static str),
    /// A `bytes32` role, by name when it is one of the known ones.
    Role(&'static str),
}

/// How one event reads. `action` opens the sentence and names a failure
/// ("Approve failed"), so it is a verb phrase, not a noun.
struct Phrase {
    signature: &'static str,
    /// Short machine-readable label, mirroring the official explorer's.
    kind: &'static str,
    action: &'static str,
    slots: &'static [Slot],
    /// Secondary `(label, slot)` pairs, shown under the sentence.
    notes: &'static [(&'static str, Slot)],
}

const fn phrase(
    signature: &'static str,
    kind: &'static str,
    action: &'static str,
    slots: &'static [Slot],
) -> Phrase {
    Phrase {
        signature,
        kind,
        action,
        slots,
        notes: &[],
    }
}

impl Phrase {
    /// The same phrase, with details shown beneath the sentence.
    const fn notes(self, notes: &'static [(&'static str, Slot)]) -> Self {
        Self { notes, ..self }
    }
}

/// Every event the built-in ABIs declare, and how it reads.
///
/// `every_registered_event_has_a_phrase` holds this to the registry, so
/// re-syncing the ABIs cannot leave a new event unexplained.
static PHRASES: &[Phrase] = &[
    // ---- TIP-20 --------------------------------------------------------
    phrase(
        "Transfer(address,address,uint256)",
        "send",
        "Send",
        &[Slot::Amount("amount"), Slot::Word("to"), Slot::Account("to")],
    ),
    phrase(
        "TransferWithMemo(address,address,uint256,bytes32)",
        "send",
        "Send",
        &[Slot::Amount("amount"), Slot::Word("to"), Slot::Account("to")],
    ),
    phrase(
        "Mint(address,uint256)",
        "mint",
        "Mint",
        &[Slot::Amount("amount"), Slot::Word("to"), Slot::Account("to")],
    ),
    phrase(
        "Burn(address,uint256)",
        "burn",
        "Burn",
        &[
            Slot::Amount("amount"),
            Slot::Word("from"),
            Slot::Account("from"),
        ],
    ),
    phrase(
        "BurnBlocked(address,uint256)",
        "burn blocked",
        "Burn Blocked",
        &[
            Slot::Amount("amount"),
            Slot::Word("from"),
            Slot::Account("from"),
        ],
    ),
    phrase(
        "Approval(address,address,uint256)",
        "approval",
        "Approve",
        &[
            Slot::Amount("amount"),
            Slot::Word("for spender"),
            Slot::Account("spender"),
        ],
    ),
    phrase(
        "RewardDistributed(address,uint256)",
        "reward distributed",
        "Distribute Reward",
        &[
            Slot::Amount("amount"),
            Slot::Word("from"),
            Slot::Account("funder"),
        ],
    ),
    phrase(
        "RewardRecipientSet(address,address)",
        "reward recipient set",
        "Set Reward Recipient",
        &[
            Slot::Account("recipient"),
            Slot::Word("for holder"),
            Slot::Account("holder"),
        ],
    ),
    phrase(
        "RoleMembershipUpdated(bytes32,address,address,bool)",
        "role membership",
        "Grant Role",
        &[
            Slot::Role("role"),
            Slot::Word("to"),
            Slot::Account("account"),
        ],
    )
    .notes(&[("Sender", Slot::Account("sender"))]),
    phrase(
        "RoleAdminUpdated(bytes32,bytes32,address)",
        "role admin updated",
        "Update Role Admin",
        &[
            Slot::Role("role"),
            Slot::Word("to"),
            Slot::Role("newAdminRole"),
        ],
    )
    .notes(&[("Sender", Slot::Account("sender"))]),
    phrase(
        "PauseStateUpdate(address,bool)",
        "pause",
        "Pause Transfers",
        &[],
    )
    .notes(&[("Updater", Slot::Account("updater"))]),
    phrase(
        "SupplyCapUpdate(address,uint256)",
        "supply cap update",
        "Update Supply Cap",
        &[],
    )
    .notes(&[
            ("New", Slot::Amount("newSupplyCap")),
            ("Updater", Slot::Account("updater")),
        ]),
    phrase(
        "TransferPolicyUpdate(address,uint64)",
        "transfer policy update",
        "Update Transfer Policy",
        &[Slot::Word("to"), Slot::Value("newPolicyId")],
    )
    .notes(&[("Updater", Slot::Account("updater"))]),
    phrase(
        "QuoteTokenUpdate(address,address)",
        "quote token update",
        "Update Quote Token",
        &[Slot::Word("to"), Slot::Token("newQuoteToken")],
    )
    .notes(&[("Updater", Slot::Account("updater"))]),
    phrase(
        "NextQuoteTokenSet(address,address)",
        "next quote token set",
        "Set Next Quote Token",
        &[Slot::Word("to"), Slot::Token("nextQuoteToken")],
    )
    .notes(&[("Updater", Slot::Account("updater"))]),
    phrase(
        "LogoURIUpdated(address,string)",
        "logo updated",
        "Update Logo",
        &[],
    )
    .notes(&[
            ("URI", Slot::Value("newLogoURI")),
            ("Updater", Slot::Account("updater")),
        ]),
    // ---- TIP-20 factory ------------------------------------------------
    phrase(
        "TokenCreated(address,string,string,string,address,address,bytes32)",
        "create token",
        "Create Token",
        &[Slot::Value("symbol")],
    )
    .notes(&[
            ("Name", Slot::Value("name")),
            ("Currency", Slot::Value("currency")),
            ("Admin", Slot::Account("admin")),
        ]),
    // ---- TIP-403 registry ----------------------------------------------
    phrase(
        "PolicyCreated(uint64,address,uint8)",
        "policy created",
        "Create Transfer Policy",
        &[Slot::Value("policyId")],
    ),
    phrase(
        "CompoundPolicyCreated(uint64,address,uint64,uint64,uint64)",
        "compound policy created",
        "Create Compound Policy",
        &[Slot::Value("policyId")],
    ),
    phrase(
        "PolicyAdminUpdated(uint64,address,address)",
        "policy admin updated",
        "Update Policy Admin",
        &[
            Slot::Word("of policy"),
            Slot::Value("policyId"),
            Slot::Word("to"),
            Slot::Account("admin"),
        ],
    ),
    phrase(
        "BlacklistUpdated(uint64,address,address,bool)",
        "blacklist updated",
        "Blacklist",
        &[
            Slot::Account("account"),
            Slot::Word("under policy"),
            Slot::Value("policyId"),
        ],
    )
    .notes(&[("Updater", Slot::Account("updater"))]),
    phrase(
        "WhitelistUpdated(uint64,address,address,bool)",
        "whitelist updated",
        "Whitelist",
        &[
            Slot::Account("account"),
            Slot::Word("under policy"),
            Slot::Value("policyId"),
        ],
    )
    .notes(&[("Updater", Slot::Account("updater"))]),
    phrase(
        "ReceivePolicyUpdated(address,uint64,uint64,address)",
        "receive policy updated",
        "Update Receive Policy",
        &[Slot::Word("for"), Slot::Account("account")],
    )
    .notes(&[
            ("Sender Policy", Slot::Value("senderPolicyId")),
            ("Token Filter", Slot::Value("tokenFilterId")),
            ("Recovery", Slot::Account("recoveryAuthority")),
        ]),
    // ---- Receive policy guard ------------------------------------------
    phrase(
        "TransferBlocked(address,address,uint64,uint256,uint8,bytes)",
        "transfer blocked",
        "Transfer Blocked",
        &[
            Slot::AmountOf {
                value: "amount",
                token: "token",
            },
            Slot::Word("to"),
            Slot::Account("receiver"),
        ],
    ),
    phrase(
        "ReceiptClaimed(address,address,uint64,uint64,uint8,address,address,address,address,address,uint256)",
        "receipt claimed",
        "Claim Blocked Transfer",
        &[
            Slot::AmountOf {
                value: "amount",
                token: "token",
            },
            Slot::Word("to"),
            Slot::Account("to"),
        ],
    )
    .notes(&[
            ("Receiver", Slot::Account("receiver")),
            ("Originator", Slot::Account("originator")),
        ]),
    phrase(
        "ReceiptBurned(address,address,uint64,uint64,uint8,address,address,address,address,uint256)",
        "receipt burned",
        "Burn Blocked Receipt",
        &[
            Slot::AmountOf {
                value: "amount",
                token: "token",
            },
            Slot::Word("for"),
            Slot::Account("receiver"),
        ],
    )
    .notes(&[("Originator", Slot::Account("originator"))]),
    // ---- Fee manager / fee AMM -----------------------------------------
    phrase(
        "FeesDistributed(address,address,uint256)",
        "fees distributed",
        "Distribute Fees",
        &[
            Slot::AmountOf {
                value: "amount",
                token: "token",
            },
            Slot::Word("to validator"),
            Slot::Account("validator"),
        ],
    ),
    phrase(
        "UserTokenSet(address,address)",
        "user token set",
        "Set Fee Token",
        &[
            Slot::Token("token"),
            Slot::Word("for"),
            Slot::Account("user"),
        ],
    ),
    phrase(
        "ValidatorTokenSet(address,address)",
        "validator token set",
        "Set Validator Fee Token",
        &[
            Slot::Token("token"),
            Slot::Word("for"),
            Slot::Account("validator"),
        ],
    ),
    phrase(
        "Mint(address,address,address,address,uint256,uint256)",
        "add liquidity",
        "Add Fee Liquidity",
        &[
            Slot::AmountOf {
                value: "amountValidatorToken",
                token: "validatorToken",
            },
            Slot::Word("for"),
            Slot::Account("to"),
        ],
    )
    .notes(&[("Liquidity", Slot::Value("liquidity"))]),
    phrase(
        "Burn(address,address,address,uint256,uint256,uint256,address)",
        "remove liquidity",
        "Remove Fee Liquidity",
        &[
            Slot::AmountOf {
                value: "amountValidatorToken",
                token: "validatorToken",
            },
            Slot::Word("to"),
            Slot::Account("to"),
        ],
    )
    .notes(&[("Liquidity", Slot::Value("liquidity"))]),
    phrase(
        "RebalanceSwap(address,address,address,uint256,uint256)",
        "rebalance swap",
        "Rebalance",
        &[
            Slot::AmountOf {
                value: "amountIn",
                token: "userToken",
            },
            Slot::Word("for"),
            Slot::AmountOf {
                value: "amountOut",
                token: "validatorToken",
            },
        ],
    ),
    // ---- Stablecoin DEX -------------------------------------------------
    phrase(
        "OrderPlaced(uint128,address,address,uint128,bool,int16,bool,int16)",
        "order placed",
        "Limit Buy",
        &[
            Slot::AmountOf {
                value: "amount",
                token: "token",
            },
            Slot::Word("at tick"),
            Slot::Value("tick"),
        ],
    )
    .notes(&[("Order", Slot::Value("orderId"))]),
    phrase(
        "OrderFilled(uint128,address,address,uint128,bool)",
        "order filled",
        "Fill Order",
        &[Slot::Value("orderId")],
    )
    .notes(&[
            ("Maker", Slot::Account("maker")),
            ("Taker", Slot::Account("taker")),
            ("Filled", Slot::Value("amountFilled")),
        ]),
    phrase(
        "OrderFlipped(uint128,address,address,uint128,bool,int16,int16)",
        "order flipped",
        "Flip Order",
        &[
            Slot::Value("orderId"),
            Slot::Word("to tick"),
            Slot::Value("flipTick"),
        ],
    )
    .notes(&[("Maker", Slot::Account("maker"))]),
    phrase(
        "OrderCancelled(uint128)",
        "order cancelled",
        "Cancel Order",
        &[Slot::Value("orderId")],
    ),
    phrase(
        "FlipFailed(uint128,address,bytes4)",
        "flip failed",
        "Order Flip Failed",
        &[Slot::Value("orderId")],
    )
    .notes(&[("Reason", Slot::Hex("reason"))]),
    phrase(
        "PairCreated(bytes32,address,address)",
        "pair created",
        "Create Pair",
        &[
            Slot::Token("base"),
            Slot::Word("/"),
            Slot::Token("quote"),
        ],
    ),
    // ---- Account keychain ------------------------------------------------
    phrase(
        "KeyAuthorized(address,address,uint8,uint64)",
        "key authorized",
        "Authorize Key",
        &[
            Slot::Account("publicKey"),
            Slot::Word("for"),
            Slot::Account("account"),
        ],
    )
    .notes(&[("Expiry", Slot::Value("expiry"))]),
    phrase(
        "AdminKeyAuthorized(address,address)",
        "admin key authorized",
        "Authorize Admin Key",
        &[
            Slot::Account("publicKey"),
            Slot::Word("for"),
            Slot::Account("account"),
        ],
    ),
    phrase(
        "KeyRevoked(address,address)",
        "key revoked",
        "Revoke Key",
        &[
            Slot::Account("publicKey"),
            Slot::Word("for"),
            Slot::Account("account"),
        ],
    ),
    phrase(
        "KeyAuthorizationWitness(address,bytes32)",
        "key witness",
        "Record Key Witness",
        &[Slot::Hex("witness")],
    ),
    phrase(
        "KeyAuthorizationWitnessBurned(address,bytes32)",
        "key witness burned",
        "Burn Key Witness",
        &[Slot::Hex("witness")],
    ),
    phrase(
        "AccessKeySpend(address,address,address,uint256,uint256)",
        "access key spend",
        "Spend From Access Key",
        &[
            Slot::AmountOf {
                value: "amount",
                token: "token",
            },
            Slot::Word("by"),
            Slot::Account("publicKey"),
        ],
    )
    .notes(&[(
            "Remaining",
            Slot::AmountOf {
                value: "remainingLimit",
                token: "token",
            },
        )]),
    phrase(
        "SpendingLimitUpdated(address,address,address,uint256)",
        "spending limit updated",
        "Update Spending Limit",
        &[
            Slot::Word("for"),
            Slot::Account("publicKey"),
            Slot::Word("to"),
            Slot::AmountOf {
                value: "newLimit",
                token: "token",
            },
        ],
    )
    .notes(&[("Account", Slot::Account("account"))]),
    // ---- Nonce / address registry ----------------------------------------
    phrase(
        "NonceIncremented(address,uint256,uint64)",
        "nonce incremented",
        "Increment Nonce",
        &[Slot::Word("for"), Slot::Account("account")],
    )
    .notes(&[("New Nonce", Slot::Value("newNonce"))]),
    phrase(
        "MasterRegistered(bytes4,address)",
        "master registered",
        "Register Virtual Master",
        &[
            Slot::Hex("masterId"),
            Slot::Word("to"),
            Slot::Account("masterAddress"),
        ],
    ),
    // ---- Payment channels -------------------------------------------------
    phrase(
        "ChannelOpened(bytes32,address,address,address,address,address,bytes32,bytes32,uint96)",
        "channel opened",
        "Open Payment Channel",
        &[
            Slot::AmountOf {
                value: "deposit",
                token: "token",
            },
            Slot::Word("to"),
            Slot::Account("payee"),
        ],
    )
    .notes(&[("Channel", Slot::Hex("channelId"))]),
    phrase(
        "Settled(bytes32,address,address,uint96,uint96,uint96)",
        "channel settled",
        "Settle Channel",
        &[Slot::Value("deltaPaid"), Slot::Word("to"), Slot::Account("payee")],
    )
    .notes(&[("Channel", Slot::Hex("channelId"))]),
    phrase(
        "TopUp(bytes32,address,address,uint96,uint96)",
        "channel top up",
        "Top Up Channel",
        &[Slot::Value("additionalDeposit")],
    )
    .notes(&[("Channel", Slot::Hex("channelId"))]),
    phrase(
        "CloseRequested(bytes32,address,address,uint256)",
        "channel close requested",
        "Request Channel Close",
        &[],
    )
    .notes(&[("Channel", Slot::Hex("channelId"))]),
    phrase(
        "CloseRequestCancelled(bytes32,address,address)",
        "channel close cancelled",
        "Cancel Channel Close",
        &[],
    )
    .notes(&[("Channel", Slot::Hex("channelId"))]),
    phrase(
        "ChannelClosed(bytes32,address,address,uint96,uint96)",
        "channel closed",
        "Close Channel",
        &[
            Slot::Value("settledToPayee"),
            Slot::Word("to"),
            Slot::Account("payee"),
        ],
    )
    .notes(&[("Refunded", Slot::Value("refundedToPayer"))]),
    // ---- Validator config -------------------------------------------------
    phrase(
        "ValidatorAdded(uint64,address,bytes32,string,string,address)",
        "validator added",
        "Add Validator",
        &[Slot::Account("validatorAddress")],
    )
    .notes(&[("Index", Slot::Value("index"))]),
    phrase(
        "ValidatorDeactivated(uint64,address)",
        "validator deactivated",
        "Deactivate Validator",
        &[Slot::Account("validatorAddress")],
    ),
    phrase(
        "ValidatorMigrated(uint64,address,bytes32)",
        "validator migrated",
        "Migrate Validator",
        &[Slot::Account("validatorAddress")],
    ),
    phrase(
        "SkippedValidatorMigration(uint64,address,bytes32)",
        "validator migration skipped",
        "Skip Validator Migration",
        &[Slot::Account("validatorAddress")],
    ),
    phrase(
        "ValidatorRotated(uint64,uint64,address,bytes32,bytes32,string,string,address)",
        "validator rotated",
        "Rotate Validator",
        &[Slot::Account("validatorAddress")],
    )
    .notes(&[("Index", Slot::Value("index"))]),
    phrase(
        "ValidatorOwnershipTransferred(uint64,address,address,address)",
        "validator ownership transferred",
        "Transfer Validator Ownership",
        &[
            Slot::Account("oldAddress"),
            Slot::Word("to"),
            Slot::Account("newAddress"),
        ],
    ),
    phrase(
        "FeeRecipientUpdated(uint64,address,address)",
        "fee recipient updated",
        "Update Fee Recipient",
        &[Slot::Word("to"), Slot::Account("feeRecipient")],
    ),
    phrase(
        "IpAddressesUpdated(uint64,string,string,address)",
        "ip addresses updated",
        "Update Validator Addresses",
        &[],
    )
    .notes(&[
            ("Ingress", Slot::Value("ingress")),
            ("Egress", Slot::Value("egress")),
        ]),
    phrase(
        "NetworkIdentityRotationEpochSet(uint64,uint64)",
        "rotation epoch set",
        "Set Rotation Epoch",
        &[Slot::Word("to"), Slot::Value("nextEpoch")],
    ),
    phrase(
        "OwnershipTransferred(address,address)",
        "ownership transferred",
        "Transfer Ownership",
        &[Slot::Word("to"), Slot::Account("newOwner")],
    ),
    phrase(
        "Initialized(uint64)",
        "initialized",
        "Initialize",
        &[Slot::Word("at height"), Slot::Value("height")],
    ),
    // ---- CreateX / Permit2 -------------------------------------------------
    phrase(
        "ContractCreation(address)",
        "contract created",
        "Deploy Contract",
        &[Slot::Account("newContract")],
    ),
    phrase(
        "ContractCreation(address,bytes32)",
        "contract created",
        "Deploy Contract",
        &[Slot::Account("newContract")],
    )
    .notes(&[("Salt", Slot::Hex("salt"))]),
    phrase(
        "Create3ProxyContractCreation(address,bytes32)",
        "proxy created",
        "Deploy CREATE3 Proxy",
        &[Slot::Account("newContract")],
    )
    .notes(&[("Salt", Slot::Hex("salt"))]),
    phrase(
        "Approval(address,address,address,uint160,uint48)",
        "approval",
        "Approve",
        &[
            Slot::AmountOf {
                value: "amount",
                token: "token",
            },
            Slot::Word("for spender"),
            Slot::Account("spender"),
        ],
    )
    .notes(&[("Expires", Slot::Value("expiration"))]),
    phrase(
        "Permit(address,address,address,uint160,uint48,uint48)",
        "permit",
        "Permit",
        &[
            Slot::Account("spender"),
            Slot::Word("to spend"),
            Slot::AmountOf {
                value: "amount",
                token: "token",
            },
        ],
    )
    .notes(&[("Expires", Slot::Value("expiration"))]),
    phrase(
        "Lockdown(address,address,address)",
        "lockdown",
        "Revoke Approval",
        &[
            Slot::Word("for"),
            Slot::Account("spender"),
            Slot::Word("on"),
            Slot::Token("token"),
        ],
    ),
    phrase(
        "NonceInvalidation(address,address,address,uint48,uint48)",
        "nonce invalidated",
        "Invalidate Nonce",
        &[Slot::Word("to"), Slot::Value("newNonce")],
    )
    .notes(&[("Spender", Slot::Account("spender"))]),
    phrase(
        "UnorderedNonceInvalidation(address,uint256,uint256)",
        "nonce invalidated",
        "Invalidate Nonces",
        &[Slot::Word("in word"), Slot::Value("word")],
    )
    .notes(&[("Owner", Slot::Account("owner"))]),
    // ---- Anchoring (0x…0a00) ----------------------------------------------
    phrase(
        "AddRegistry(address,uint64,string)",
        "registry added",
        "Add Registry",
        &[Slot::Value("name")],
    )
    .notes(&[("Registry", Slot::Value("registryId"))]),
    phrase(
        "AddRecord(address,uint64,uint64,uint64,string)",
        "record added",
        "Add Record",
        &[
            Slot::Word("version"),
            Slot::Value("index"),
            Slot::Word("of"),
            Slot::Value("checksum"),
        ],
    )
    .notes(&[
        ("Registry", Slot::Value("registryId")),
        ("Record", Slot::Value("recordId")),
    ]),
    phrase(
        "UpdateRecordStatus(address,uint64,uint64,uint64,string)",
        "record status updated",
        "Update Record Status",
        &[Slot::Value("status"), Slot::Word("on version"), Slot::Value("index")],
    )
    .notes(&[
        ("Registry", Slot::Value("registryId")),
        ("Record", Slot::Value("recordId")),
    ]),
    phrase(
        "GrantRole(address,uint64,string,address,string)",
        "role granted",
        "Grant Role",
        &[Slot::Value("role"), Slot::Word("to"), Slot::Account("account")],
    )
    .notes(&[
        ("Registry", Slot::Value("registryId")),
        ("Record", Slot::Value("checksum")),
    ]),
    phrase(
        "RevokeRole(address,uint64,string,address,string)",
        "role revoked",
        "Revoke Role",
        &[Slot::Value("role"), Slot::Word("from"), Slot::Account("account")],
    )
    .notes(&[
        ("Registry", Slot::Value("registryId")),
        ("Record", Slot::Value("checksum")),
    ]),
];

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

/// The TIP-20 roles, so `0xdf8b4c52…` reads as `ISSUER_ROLE`.
const KNOWN_ROLES: &[&str] = &[
    "DEFAULT_ADMIN_ROLE",
    "ISSUER_ROLE",
    "PAUSE_ROLE",
    "UNPAUSE_ROLE",
    "BURN_BLOCKED_ROLE",
];

/// What a `bytes32` role says it is. TIP-20 hashes its role names, so a word is
/// matched against the ones this explorer knows.
fn role_name(word: &str) -> Option<String> {
    let hashed = word.to_lowercase();
    KNOWN_ROLES
        .iter()
        .find(|role| keccak_hex(role.as_bytes()) == hashed)
        .map(|role| (*role).to_string())
}

// ---------------------------------------------------------------------------
// Known events
// ---------------------------------------------------------------------------

/// One log, said in words.
#[derive(Debug, Clone, Serialize)]
pub struct KnownEvent {
    /// Machine-readable label ("send", "mint", …).
    pub kind: String,
    /// The verb phrase alone, for a failure headline.
    pub action: String,
    /// The whole sentence.
    pub headline: String,
    /// A memo or other free-form note the sender attached.
    pub note: Option<String>,
    /// `(label, value)` details shown beneath the sentence.
    pub details: Vec<(String, String)>,
    pub from: Option<String>,
    pub to: Option<String>,
    /// A TIP-20 transfer into the fee manager — the fee, not the payment.
    pub is_fee: bool,
    /// Position in the receipt's log list, so the UI can pair the two.
    pub log_index: usize,
}

/// Whether an event is worth leading with. Bookkeeping the chain does beside
/// the actual work — a nonce bump, a key check — is not.
fn is_preferred(event: &KnownEvent) -> bool {
    !matches!(
        event.kind.as_str(),
        "key authorized" | "key revoked" | "nonce incremented" | "access key spend"
    )
}

fn token_display<'t>(tokens: &'t Tokens, address: &str) -> Option<&'t TokenDisplay> {
    tokens.get(&address.to_lowercase())
}

/// An amount in the token's own units. Without metadata the raw integer
/// stands: a wrong scale is worse than an unscaled one.
fn format_amount(tokens: &Tokens, token: &str, raw: &str) -> String {
    match token_display(tokens, token) {
        Some(meta) => {
            let amount = format_token_amount(raw, meta.decimals);
            if meta.symbol.is_empty() {
                amount
            } else {
                format!("{amount} {}", meta.symbol)
            }
        }
        None => raw.to_string(),
    }
}

fn render_slot(slot: &Slot, event: &DecodedEvent, tokens: &Tokens) -> Option<String> {
    Some(match slot {
        Slot::Word(word) => (*word).to_string(),
        Slot::Amount(name) => format_amount(tokens, &event.contract, event.param(name)?),
        Slot::AmountOf { value, token } => {
            format_amount(tokens, event.param(token)?, event.param(value)?)
        }
        Slot::Account(name) => truncate(event.param(name)?),
        Slot::Token(name) => {
            let address = event.param(name)?;
            match token_display(tokens, address) {
                Some(meta) if !meta.symbol.is_empty() => meta.symbol.clone(),
                _ => truncate(address),
            }
        }
        Slot::Value(name) => event.param(name)?.to_string(),
        Slot::Hex(name) => truncate(event.param(name)?),
        Slot::Role(name) => {
            let word = event.param(name)?;
            role_name(word).unwrap_or_else(|| truncate(word))
        }
    })
}

fn phrase_for(signature: &str) -> Option<&'static Phrase> {
    PHRASES.iter().find(|p| p.signature == signature)
}

/// Say what one decoded log did. `None` for a log with no phrasing.
fn known_event(
    event: &DecodedEvent,
    log_index: usize,
    tokens: &Tokens,
    sender: Option<&str>,
) -> Option<KnownEvent> {
    let signature = event.signature.as_deref()?;
    let definition = phrase_for(signature)?;

    let mut known = KnownEvent {
        kind: definition.kind.to_string(),
        action: definition.action.to_string(),
        headline: String::new(),
        note: None,
        details: definition
            .notes
            .iter()
            .filter_map(|(label, slot)| {
                render_slot(slot, event, tokens).map(|v| ((*label).to_string(), v))
            })
            .filter(|(_, value)| !value.is_empty())
            .collect(),
        from: event.param("from").map(String::from),
        to: event.param("to").map(String::from),
        is_fee: false,
        log_index,
    };

    refine(&mut known, event, sender);

    let rest: Vec<String> = definition
        .slots
        .iter()
        .filter_map(|slot| render_slot(slot, event, tokens))
        .collect();
    known.headline = if rest.is_empty() {
        known.action.clone()
    } else {
        format!("{} {}", known.action, rest.join(" "))
    };
    Some(known)
}

/// Adjust events whose meaning turns on an argument: a boolean flag, a
/// counterparty, an address format.
fn refine(known: &mut KnownEvent, event: &DecodedEvent, sender: Option<&str>) {
    let flag = |name: &str| event.param(name) == Some("true");

    match known.kind.as_str() {
        "send" => {
            let from = known.from.clone().unwrap_or_default();
            let to = known.to.clone().unwrap_or_default();
            // A transfer into the fee manager is the fee for this very
            // transaction, not something the sender set out to do.
            if same_address(&to, FEE_MANAGER_ADDRESS) && !same_address(&from, ZERO_ADDRESS) {
                known.kind = "fee transfer".into();
                known.action = "Pay Fee".into();
                known.is_fee = true;
                return;
            }
            match event.param("memo").map_or(Memo::Nothing, memo::read) {
                Memo::Attribution => {
                    known.action = "MPP Payment".into();
                    return;
                }
                Memo::Note(note) => known.note = Some(note),
                Memo::Nothing => {}
            }
            // Funds arriving at a TIP-1022 deposit address are credited to the
            // master wallet, so the transfer is a forward, not a send.
            if is_virtual(&from) {
                known.action = "Forwarded".into();
            }
        }
        "mint" => {
            let to = event.param("to").unwrap_or_default();
            if let Some(sender) = sender {
                if !same_address(sender, to) {
                    known.action = "Mint to Recipient".into();
                }
            }
        }
        "role membership" => {
            if !flag("hasRole") {
                known.kind = "revoke role".into();
                known.action = "Revoke Role".into();
            } else {
                known.kind = "grant role".into();
            }
        }
        "pause" => {
            if !flag("isPaused") {
                known.kind = "unpause".into();
                known.action = "Resume Transfers".into();
            }
        }
        "blacklist updated" => {
            if !flag("restricted") {
                known.action = "Remove From Blacklist".into();
            }
        }
        "whitelist updated" => {
            if !flag("allowed") {
                known.action = "Remove From Whitelist".into();
            }
        }
        "order placed" => {
            let side = if flag("isBid") { "Buy" } else { "Sell" };
            if flag("isFlipOrder") {
                known.kind = "flip order placed".into();
                known.action = format!("Flip {side}");
            } else {
                known.action = format!("Limit {side}");
            }
        }
        "order flipped" => {
            let side = if flag("isBid") { "Buy" } else { "Sell" };
            known.action = format!("Flip Order to {side}");
        }
        _ => {}
    }
}

/// Address equality that ignores checksum casing.
fn same_address(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// Say what every log in a receipt did, in receipt order.
pub fn known_events(
    events: &[DecodedEvent],
    tokens: &Tokens,
    sender: Option<&str>,
) -> Vec<KnownEvent> {
    events
        .iter()
        .enumerate()
        .filter_map(|(i, e)| known_event(e, i, tokens, sender))
        .collect()
}

// ---------------------------------------------------------------------------
// Transaction summary
// ---------------------------------------------------------------------------

/// The one-line verdict at the top of a transaction page.
#[derive(Debug, Clone, Serialize)]
pub struct TxSummary {
    /// `success`, `failure`, or `neutral` — what the card is coloured by.
    pub tone: String,
    pub headline: String,
    pub details: Vec<String>,
    /// The failure in words, when there was one.
    pub error: Option<String>,
    /// The node's own message, kept when it says more than we could.
    pub raw_reason: Option<String>,
}

/// What is known about why a transaction failed.
#[derive(Debug, Clone, Default)]
pub struct Failure {
    /// ABI-encoded revert data, if the node returned any.
    pub revert_data: Option<String>,
    /// The node's free-form message.
    pub reason: Option<String>,
    /// The function the failing call was invoking.
    pub function: Option<String>,
    /// A label for the contract that failed.
    pub contract: Option<String>,
    /// The token the failing call was about, for a balance error.
    pub token: Option<String>,
}

/// Build the summary for a transaction.
pub fn build_summary(
    succeeded: bool,
    events: &[KnownEvent],
    failure: Option<&Failure>,
    tokens: &Tokens,
) -> TxSummary {
    if !succeeded {
        return failure_summary(failure.unwrap_or(&Failure::default()), events, tokens);
    }

    // Fee transfers happen on every transaction; they are never the point of
    // one, so they only lead when nothing else did anything. Ties go to
    // receipt order.
    let leading = events.iter().min_by_key(|e| (!is_preferred(e), e.is_fee));

    let Some(event) = leading else {
        return TxSummary {
            tone: "success".into(),
            headline: "Transaction succeeded.".into(),
            details: vec!["No high-level events were detected for this transaction.".into()],
            error: None,
            raw_reason: None,
        };
    };

    let interpreted = events.len();
    TxSummary {
        tone: "success".into(),
        headline: format!("{}.", event.headline),
        details: vec![format!(
            "{interpreted} interpreted event{} detected.",
            if interpreted == 1 { "" } else { "s" }
        )],
        error: None,
        raw_reason: None,
    }
}

fn failure_summary(failure: &Failure, events: &[KnownEvent], tokens: &Tokens) -> TxSummary {
    // Revert data may arrive as a field or embedded in the node's prose.
    let revert_data = failure
        .revert_data
        .clone()
        .or_else(|| failure.reason.as_deref().and_then(revert_data_in));
    let decoded = revert_data.as_deref().and_then(decode_revert);
    let raw_reason = failure
        .reason
        .clone()
        .or_else(|| decoded.as_ref().map(|d| d.call_form()));

    if let Some(summary) =
        insufficient_balance_summary(decoded.as_ref(), failure, tokens, raw_reason.as_deref())
    {
        return summary;
    }

    let action = failure
        .function
        .as_deref()
        .map(|f| format!("{} failed", sentence_case(f)))
        .or_else(|| {
            events
                .iter()
                .find(|e| is_preferred(e))
                .map(|e| format!("{} failed", e.action))
        })
        .unwrap_or_else(|| "Transaction failed".into());

    // The decoded error names the objection; the node's prose is the
    // fallback, and only when it is prose rather than a hex blob.
    let error = decoded
        .as_ref()
        .and_then(|d| {
            d.reason()
                .map(String::from)
                .or_else(|| Some(humanize_identifier(&d.name)))
        })
        .or_else(|| failure.reason.as_deref().and_then(humanize_raw_reason));

    let mut details = Vec::new();
    match (failure.contract.as_deref(), failure.function.as_deref()) {
        (Some(contract), Some(function)) => {
            details.push(format!("Failed call: {contract}.{function}()."))
        }
        (Some(contract), None) => details.push(format!("Failed contract: {contract}.")),
        _ => {}
    }
    if let Some(decoded) = &decoded {
        if decoded.reason().is_none() && !decoded.params.is_empty() {
            details.push(format!("Revert: {}", decoded.call_form()));
        }
    }
    if let Some(raw) = &raw_reason {
        if Some(raw.as_str()) != error.as_deref() {
            details.push(format!("Raw reason: {raw}"));
        }
    }

    TxSummary {
        tone: "failure".into(),
        headline: format!("{action}."),
        details,
        error,
        raw_reason,
    }
}

/// The one failure worth spelling out in full. The TIP-20 error carries what
/// was available and what was needed, which is the question the reader has.
fn insufficient_balance_summary(
    decoded: Option<&DecodedError>,
    failure: &Failure,
    tokens: &Tokens,
    raw_reason: Option<&str>,
) -> Option<TxSummary> {
    let name = decoded.map(|d| d.name.as_str()).unwrap_or_default();
    let text = format!("{name} {}", raw_reason.unwrap_or_default()).to_lowercase();
    if !text.contains("insufficient") || !text.contains("balance") {
        return None;
    }

    // `InsufficientBalance(available, required, token)` — the token argument
    // is more reliable than the address the call was aimed at.
    let token = decoded
        .and_then(|d| {
            d.params
                .iter()
                .find(|p| p.ty == "address" && is_valid_address(&p.value))
        })
        .map(|p| p.value.clone())
        .or_else(|| failure.token.as_deref().map(checksum_address));
    let label = match token.as_deref().and_then(|t| token_display(tokens, t)) {
        Some(meta) if !meta.symbol.is_empty() => format!("{} balance", meta.symbol),
        _ if token.is_some() => "TIP-20 balance".into(),
        _ => "token balance".into(),
    };

    // Available before required, as the error declares them.
    let numbers: Vec<&DecodedParam> = decoded
        .map(|d| {
            d.params
                .iter()
                .filter(|p| p.ty.starts_with("uint"))
                .collect()
        })
        .unwrap_or_default();
    let amounts = match numbers.as_slice() {
        [.., available, required] => {
            let show = |p: &DecodedParam| {
                format_amount(tokens, token.as_deref().unwrap_or_default(), &p.value)
            };
            format!(
                " Available {}, required {}.",
                show(available),
                show(required)
            )
        }
        _ => String::new(),
    };

    let mut details = Vec::new();
    if let Some(contract) = &failure.contract {
        details.push(format!("Failed at {contract}."));
    }
    if let Some(raw) = raw_reason {
        details.push(format!("Raw reason: {raw}"));
    }

    Some(TxSummary {
        tone: "failure".into(),
        headline: format!("Transfer failed: insufficient {label}.{amounts}"),
        details,
        error: Some(format!("insufficient {label}")),
        raw_reason: raw_reason.map(String::from),
    })
}

fn sentence_case(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// `InsufficientAllowance` -> `insufficient allowance`.
fn humanize_identifier(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 4);
    for (i, c) in value.chars().enumerate() {
        if i > 0 && c.is_ascii_uppercase() {
            out.push(' ');
        }
        if c == '_' || c == '-' {
            out.push(' ');
        } else {
            out.push(c.to_ascii_lowercase());
        }
    }
    let out = out.split_whitespace().collect::<Vec<_>>().join(" ");
    // The one identifier whose humanized form should keep its spelling.
    if let Some(rest) = out.strip_prefix("tip 20 ") {
        return format!("TIP-20 {rest}");
    }
    out
}

/// The node's message without its boilerplate, or `None` when what is left is
/// a hex blob.
fn humanize_raw_reason(value: &str) -> Option<String> {
    let cleaned = value
        .trim()
        .trim_start_matches("execution reverted")
        .trim_start_matches("reverted")
        .trim_start_matches(':')
        .trim();
    if cleaned.is_empty() || cleaned.starts_with("0x") {
        return None;
    }
    Some(cleaned.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::decode_event;
    use crate::decoder::{event_signature, REGISTRY};
    use serde_json::json;

    fn token_map() -> Tokens {
        let mut tokens = Tokens::new();
        tokens.insert(
            "0x20c0000000000000000000000000000000000000".into(),
            TokenDisplay {
                symbol: "pathUSD".into(),
                decimals: 6,
            },
        );
        tokens
    }

    fn topic(address: &str) -> String {
        format!("0x{}{}", "00".repeat(12), address.trim_start_matches("0x"))
    }

    /// A TIP-20 transfer log, in the shape a receipt carries it.
    fn transfer_log(from: &str, to: &str, amount: u128) -> serde_json::Value {
        json!({
            "address": "0x20c0000000000000000000000000000000000000",
            "topics": [
                crate::decoder::TRANSFER_TOPIC.as_str(),
                topic(from),
                topic(to),
            ],
            "data": format!("0x{amount:064x}"),
            "logIndex": "0x0",
        })
    }

    fn say(log: &serde_json::Value, sender: Option<&str>) -> KnownEvent {
        let decoded = decode_event(log).expect("decoded");
        known_event(&decoded, 0, &token_map(), sender).expect("phrased")
    }

    /// The headline sentence the whole feature exists for.
    #[test]
    fn a_transfer_reads_as_a_sentence() {
        let to = "0x1111111111111111111111111111111111111111";
        let event = say(
            &transfer_log("0x2222222222222222222222222222222222222222", to, 1_500_000),
            None,
        );
        assert_eq!(event.headline, "Send 1.5 pathUSD to 0x1111…1111");
        assert_eq!(event.kind, "send");
        assert!(!event.is_fee);
    }

    #[test]
    fn an_unknown_token_keeps_its_raw_amount() {
        let mut log = transfer_log(
            "0x2222222222222222222222222222222222222222",
            "0x1111111111111111111111111111111111111111",
            1_500_000,
        );
        log["address"] = json!("0x3333333333333333333333333333333333333333");
        assert_eq!(say(&log, None).headline, "Send 1500000 to 0x1111…1111");
    }

    /// A transfer into the fee manager is the fee, and must not be mistaken
    /// for what the transaction set out to do.
    #[test]
    fn a_transfer_to_the_fee_manager_is_a_fee() {
        let event = say(
            &transfer_log(
                "0x2222222222222222222222222222222222222222",
                FEE_MANAGER_ADDRESS,
                100,
            ),
            None,
        );
        assert!(event.is_fee);
        assert_eq!(event.kind, "fee transfer");
    }

    /// A mint is a mint when you mint to yourself, and something else when you
    /// mint to someone else.
    #[test]
    fn a_mint_names_its_recipient_when_it_is_not_the_sender() {
        let to = "0x1111111111111111111111111111111111111111";
        let log = json!({
            "address": "0x20c0000000000000000000000000000000000000",
            "topics": [crate::decoder::keccak_hex(b"Mint(address,uint256)"), topic(to)],
            "data": format!("0x{:064x}", 2_000_000u128),
        });
        assert_eq!(say(&log, Some(to)).action, "Mint");
        assert_eq!(
            say(&log, Some("0x9999999999999999999999999999999999999999")).action,
            "Mint to Recipient"
        );
    }

    /// A boolean argument flips the sentence; both readings must be right.
    #[test]
    fn a_boolean_argument_decides_the_verb() {
        let role = crate::decoder::keccak_hex(b"ISSUER_ROLE");
        let account = "0x1111111111111111111111111111111111111111";
        let sender = "0x2222222222222222222222222222222222222222";
        let membership = |has_role: bool| {
            json!({
                "address": "0x20c0000000000000000000000000000000000000",
                "topics": [
                    crate::decoder::keccak_hex(b"RoleMembershipUpdated(bytes32,address,address,bool)"),
                    role,
                    topic(account),
                    topic(sender),
                ],
                "data": format!("0x{:064x}", u8::from(has_role)),
            })
        };
        let granted = say(&membership(true), None);
        assert_eq!(granted.headline, "Grant Role ISSUER_ROLE to 0x1111…1111");
        assert_eq!(granted.kind, "grant role");
        let revoked = say(&membership(false), None);
        assert_eq!(revoked.headline, "Revoke Role ISSUER_ROLE to 0x1111…1111");
        assert_eq!(revoked.kind, "revoke role");
    }

    /// A memo rides along as a note, and a binary payload does not.
    #[test]
    fn a_memo_becomes_a_note() {
        let mut memo_bytes = vec![0u8; 32];
        memo_bytes[..7].copy_from_slice(b"invoice");
        let log = json!({
            "address": "0x20c0000000000000000000000000000000000000",
            "topics": [
                crate::decoder::TRANSFER_WITH_MEMO_TOPIC.as_str(),
                topic("0x2222222222222222222222222222222222222222"),
                topic("0x1111111111111111111111111111111111111111"),
                format!("0x{}", hex::encode(&memo_bytes)),
            ],
            "data": format!("0x{:064x}", 1_000_000u128),
        });
        assert_eq!(say(&log, None).note.as_deref(), Some("invoice"));
    }

    /// A transfer out of a TIP-1022 deposit address is a forward.
    #[test]
    fn a_virtual_sender_is_forwarding() {
        let from = crate::tempo_address::virtual_address(&[0xab; 4], &[0xcd; 6]);
        let event = say(
            &transfer_log(
                &from,
                "0x1111111111111111111111111111111111111111",
                1_000_000,
            ),
            None,
        );
        assert_eq!(event.action, "Forwarded");
    }

    /// Every event the registry can decode must have a phrasing, or the
    /// events tab falls back to raw parameter lists for it.
    #[test]
    fn every_registered_event_has_a_phrase() {
        let mut missing: Vec<String> = REGISTRY
            .events()
            .map(event_signature)
            .filter(|signature| phrase_for(signature).is_none())
            .collect();
        missing.sort();
        missing.dedup();
        assert!(missing.is_empty(), "events without a phrase: {missing:#?}");
    }

    /// Conversely, a phrase for an event nothing declares is dead weight and
    /// probably a typo in a signature.
    #[test]
    fn every_phrase_matches_a_registered_event() {
        for phrase in PHRASES {
            let topic0 = crate::decoder::keccak256(phrase.signature.as_bytes());
            assert!(
                REGISTRY.event(&topic0).is_some(),
                "`{}` matches no registered event",
                phrase.signature
            );
        }
    }

    // ---- summaries ------------------------------------------------------

    fn summary_of_failure(failure: Failure) -> TxSummary {
        build_summary(false, &[], Some(&failure), &token_map())
    }

    #[test]
    fn a_successful_transaction_leads_with_what_it_did() {
        let events = vec![
            say(
                &transfer_log(
                    "0x2222222222222222222222222222222222222222",
                    FEE_MANAGER_ADDRESS,
                    10,
                ),
                None,
            ),
            say(
                &transfer_log(
                    "0x2222222222222222222222222222222222222222",
                    "0x1111111111111111111111111111111111111111",
                    1_500_000,
                ),
                None,
            ),
        ];
        let summary = build_summary(true, &events, None, &token_map());
        assert_eq!(summary.tone, "success");
        // The fee came first in the receipt but is not what the sender did.
        assert_eq!(summary.headline, "Send 1.5 pathUSD to 0x1111…1111.");
        assert_eq!(summary.details, ["2 interpreted events detected."]);
    }

    #[test]
    fn a_transaction_with_no_readable_events_still_says_so() {
        let summary = build_summary(true, &[], None, &token_map());
        assert_eq!(summary.headline, "Transaction succeeded.");
    }

    /// The failure the reader most often needs spelled out.
    #[test]
    fn an_insufficient_balance_names_the_gap() {
        let token = "0x20c0000000000000000000000000000000000000";
        let args = ethers_core::abi::encode(&[
            ethers_core::abi::Token::Uint(1_000_000u64.into()),
            ethers_core::abi::Token::Uint(2_500_000u64.into()),
            ethers_core::abi::Token::Address(token.parse().unwrap()),
        ]);
        let selector =
            &crate::decoder::keccak256(b"InsufficientBalance(uint256,uint256,address)")[..4];
        let data = format!("0x{}{}", hex::encode(selector), hex::encode(args));
        let summary = summary_of_failure(Failure {
            revert_data: Some(data),
            ..Default::default()
        });
        assert_eq!(summary.tone, "failure");
        assert_eq!(
            summary.headline,
            "Transfer failed: insufficient pathUSD balance. Available 1 pathUSD, required 2.5 pathUSD."
        );
    }

    /// `revert("…")` reaches the reader as its message.
    #[test]
    fn a_revert_string_becomes_the_error() {
        let data = format!(
            "0x08c379a0{}",
            hex::encode(ethers_core::abi::encode(&[
                ethers_core::abi::Token::String("not authorized".into())
            ]))
        );
        let summary = summary_of_failure(Failure {
            revert_data: Some(data),
            function: Some("transfer".into()),
            contract: Some("pathUSD".into()),
            ..Default::default()
        });
        assert_eq!(summary.headline, "Transfer failed.");
        assert_eq!(summary.error.as_deref(), Some("not authorized"));
        assert!(summary
            .details
            .contains(&"Failed call: pathUSD.transfer().".to_string()));
    }

    /// A custom error with no message still says something in words.
    #[test]
    fn a_custom_error_is_humanized() {
        let selector = &crate::decoder::keccak256(b"InsufficientAllowance()")[..4];
        let summary = summary_of_failure(Failure {
            revert_data: Some(format!("0x{}", hex::encode(selector))),
            ..Default::default()
        });
        assert_eq!(summary.error.as_deref(), Some("insufficient allowance"));
    }

    /// Nothing decodable at all: the headline must still be honest.
    #[test]
    fn an_undecodable_failure_falls_back() {
        let summary = summary_of_failure(Failure {
            reason: Some("execution reverted: out of gas".into()),
            ..Default::default()
        });
        assert_eq!(summary.headline, "Transaction failed.");
        assert_eq!(summary.error.as_deref(), Some("out of gas"));

        let bare = summary_of_failure(Failure::default());
        assert_eq!(bare.headline, "Transaction failed.");
        assert_eq!(bare.error, None);
    }
}
