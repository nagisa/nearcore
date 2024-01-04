use crate::hash::CryptoHash;
use crate::serialize::dec_format;
use crate::types::{Balance, Nonce, StorageUsage};
use borsh::{BorshDeserialize, BorshSerialize};
pub use near_account_id as id;
use std::io;

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Copy,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum AccountVersion {
    #[cfg_attr(not(feature = "protocol_feature_nonrefundable_transfer_nep491"), default)]
    V1,
    #[default]
    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    V2,
}

impl TryFrom<u8> for AccountVersion {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(AccountVersion::V1),
            #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
            2 => Ok(AccountVersion::V2),
            _ => Err(()),
        }
    }
}

/// Per account information stored in the state.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct Account {
    /// The total not locked, refundable tokens.
    #[serde(with = "dec_format")]
    amount: Balance,
    /// The amount locked due to staking.
    #[serde(with = "dec_format")]
    locked: Balance,
    /// Tokens that are not available to withdraw, stake, or refund, but can be used to cover storage usage.
    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    #[serde(default = "Account::default_nonrefundable")]
    #[serde(with = "dec_format")]
    nonrefundable: Balance,
    /// Hash of the code stored in the storage for this account.
    code_hash: CryptoHash,
    /// Storage used by the given account, includes account id, this struct, access keys and other data.
    storage_usage: StorageUsage,
    /// Version of Account in re migrations and similar
    ///
    /// Note(jakmeier): Why does this exist? We only have one version right now
    /// and the code doesn't allow adding a new version at all since this field
    /// is not included in the merklized state...
    #[serde(default)]
    version: AccountVersion,
}

impl Account {
    /// Max number of bytes an account can have in its state (excluding contract code)
    /// before it is infeasible to delete.
    pub const MAX_ACCOUNT_DELETION_STORAGE_USAGE: u64 = 10_000;
    /// HACK: Using u128::MAX as a sentinel value, there are not enough tokens
    /// in total supply which makes it an invalid value. We use it to
    /// differentiate AccountVersion V1 from newer versions.
    const SERIALIZATION_SENTINEL: u128 = u128::MAX;

    pub fn new(
        amount: Balance,
        locked: Balance,
        nonrefundable: Balance,
        code_hash: CryptoHash,
        storage_usage: StorageUsage,
    ) -> Self {
        #[cfg(not(feature = "protocol_feature_nonrefundable_transfer_nep491"))]
        assert_eq!(nonrefundable, 0);
        Account {
            amount,
            locked,
            #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
            nonrefundable,
            code_hash,
            storage_usage,
            version: AccountVersion::default(),
        }
    }

    #[inline]
    pub fn amount(&self) -> Balance {
        self.amount
    }

    #[inline]
    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    pub fn nonrefundable(&self) -> Balance {
        self.nonrefundable
    }

    fn default_nonrefundable() -> Balance {
        0
    }

    #[inline]
    #[cfg(not(feature = "protocol_feature_nonrefundable_transfer_nep491"))]
    pub fn nonrefundable(&self) -> Balance {
        Self::default_nonrefundable()
    }

    #[inline]
    pub fn locked(&self) -> Balance {
        self.locked
    }

    #[inline]
    pub fn code_hash(&self) -> CryptoHash {
        self.code_hash
    }

    #[inline]
    pub fn storage_usage(&self) -> StorageUsage {
        self.storage_usage
    }

    #[inline]
    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    pub fn version(&self) -> AccountVersion {
        self.version
    }

    #[inline]
    pub fn set_amount(&mut self, amount: Balance) {
        self.amount = amount;
    }

    #[inline]
    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    pub fn set_nonrefundable(&mut self, nonrefundable: Balance) {
        self.nonrefundable = nonrefundable;
    }

    #[inline]
    pub fn set_locked(&mut self, locked: Balance) {
        self.locked = locked;
    }

    #[inline]
    pub fn set_code_hash(&mut self, code_hash: CryptoHash) {
        self.code_hash = code_hash;
    }

    #[inline]
    pub fn set_storage_usage(&mut self, storage_usage: StorageUsage) {
        self.storage_usage = storage_usage;
    }

    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    pub fn set_version(&mut self, version: AccountVersion) {
        self.version = version;
    }
}

/// Note(jakmeier): Even though this is called "legacy", it looks like this is
/// the one and only serialization format of Accounts currently in use.
#[derive(BorshSerialize)]
#[cfg_attr(
    not(feature = "protocol_feature_nonrefundable_transfer_nep491"),
    derive(BorshDeserialize)
)]
struct LegacyAccount {
    amount: Balance,
    locked: Balance,
    code_hash: CryptoHash,
    storage_usage: StorageUsage,
}

#[derive(BorshSerialize)]
struct AccountV2 {
    amount: Balance,
    locked: Balance,
    code_hash: CryptoHash,
    storage_usage: StorageUsage,
    nonrefundable: Balance,
}

impl BorshDeserialize for Account {
    fn deserialize_reader<R: io::Read>(rd: &mut R) -> io::Result<Self> {
        // The first value of all Account serialization formats is a u128,
        // either a sentinel or a balance.
        let sentinel_or_amount = u128::deserialize_reader(rd)?;
        if sentinel_or_amount == Account::SERIALIZATION_SENTINEL {
            // Account v2 or newer
            let version_byte = u8::deserialize_reader(rd)?;
            if version_byte != 2 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Error deserializing account: version {} does not equal 2",
                        version_byte
                    ),
                ));
            }

            let version = AccountVersion::try_from(version_byte).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Error deserializing account: invalid account version {}",
                        version_byte
                    ),
                )
            })?;

            let amount = u128::deserialize_reader(rd)?;
            let locked = u128::deserialize_reader(rd)?;
            let code_hash = CryptoHash::deserialize_reader(rd)?;
            let storage_usage = StorageUsage::deserialize_reader(rd)?;
            #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
            let nonrefundable = u128::deserialize_reader(rd)?;

            Ok(Account {
                amount,
                locked,
                code_hash,
                storage_usage,
                version,
                #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
                nonrefundable,
            })
        } else {
            // Account v1
            let locked = u128::deserialize_reader(rd)?;
            let code_hash = CryptoHash::deserialize_reader(rd)?;
            let storage_usage = StorageUsage::deserialize_reader(rd)?;

            Ok(Account {
                amount: sentinel_or_amount,
                locked,
                code_hash,
                storage_usage,
                version: AccountVersion::V1,
                #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
                nonrefundable: 0,
            })
        }
    }
}

impl BorshSerialize for Account {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        let legacy_account = LegacyAccount {
            amount: self.amount,
            locked: self.locked,
            code_hash: self.code_hash,
            storage_usage: self.storage_usage,
        };

        #[cfg(not(feature = "protocol_feature_nonrefundable_transfer_nep491"))]
        {
            legacy_account.serialize(writer)
        }

        #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
        {
            match self.version {
                // Note(jakmeier): It might be tempting to lazily convert old V1 to V2
                // while serializing. But that would break the borsh assumptions
                // of unique binary representation.
                AccountVersion::V1 => legacy_account.serialize(writer),
                // Note(jakmeier): These accounts are serialized in merklized state.
                // I would really like to avoid migration of the MPT.
                // This here would keep old accounts in the old format
                // and only allow nonrefundable storage on new accounts.
                AccountVersion::V2 => {
                    let account = AccountV2 {
                        amount: self.amount,
                        locked: self.locked,
                        code_hash: self.code_hash,
                        storage_usage: self.storage_usage,
                        nonrefundable: self.nonrefundable,
                    };
                    let sentinel = Account::SERIALIZATION_SENTINEL;
                    // For now a constant, but if we need V3 later we can use this
                    // field instead of sentinel magic.
                    let version = 2u8;
                    BorshSerialize::serialize(&sentinel, writer)?;
                    BorshSerialize::serialize(&version, writer)?;
                    account.serialize(writer)
                }
            }
        }
    }
}

/// Access key provides limited access to an account. Each access key belongs to some account and
/// is identified by a unique (within the account) public key. One account may have large number of
/// access keys. Access keys allow to act on behalf of the account by restricting transactions
/// that can be issued.
/// `account_id,public_key` is a key in the state
#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct AccessKey {
    /// Nonce for this access key, used for tx nonce generation. When access key is created, nonce
    /// is set to `(block_height - 1) * 1e6` to avoid tx hash collision on access key re-creation.
    /// See <https://github.com/near/nearcore/issues/3779> for more details.
    pub nonce: Nonce,

    /// Defines permissions for this access key.
    pub permission: AccessKeyPermission,
}

impl AccessKey {
    pub const ACCESS_KEY_NONCE_RANGE_MULTIPLIER: u64 = 1_000_000;

    pub fn full_access() -> Self {
        Self { nonce: 0, permission: AccessKeyPermission::FullAccess }
    }
}

/// Defines permissions for AccessKey
#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum AccessKeyPermission {
    FunctionCall(FunctionCallPermission),

    /// Grants full access to the account.
    /// NOTE: It's used to replace account-level public keys.
    FullAccess,
}

/// Grants limited permission to make transactions with FunctionCallActions
/// The permission can limit the allowed balance to be spent on the prepaid gas.
/// It also restrict the account ID of the receiver for this function call.
/// It also can restrict the method name for the allowed function calls.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Debug,
)]
pub struct FunctionCallPermission {
    /// Allowance is a balance limit to use by this access key to pay for function call gas and
    /// transaction fees. When this access key is used, both account balance and the allowance is
    /// decreased by the same value.
    /// `None` means unlimited allowance.
    /// NOTE: To change or increase the allowance, the old access key needs to be deleted and a new
    /// access key should be created.
    #[serde(with = "dec_format")]
    pub allowance: Option<Balance>,

    // This isn't an AccountId because already existing records in testnet genesis have invalid
    // values for this field (see: https://github.com/near/nearcore/pull/4621#issuecomment-892099860)
    // we accomodate those by using a string, allowing us to read and parse genesis.
    /// The access key only allows transactions with the given receiver's account id.
    pub receiver_id: String,

    /// A list of method names that can be used. The access key only allows transactions with the
    /// function call of one of the given method names.
    /// Empty list means any method name can be used.
    pub method_names: Vec<String>,
}

#[cfg(test)]
mod tests {

    use crate::hash::hash;

    use super::*;

    #[test]
    fn test_account_serialization() {
        let acc = Account::new(1_000_000, 1_000_000, 0, CryptoHash::default(), 100);
        let bytes = borsh::to_vec(&acc).unwrap();
        if cfg!(feature = "protocol_feature_nonrefundable_transfer_nep491") {
            expect_test::expect!("HaZPNG4KpXQ9Mre4PAA83V5usqXsA4zy4vMwSXBiBcQv")
        } else {
            expect_test::expect!("EVk5UaxBe8LQ8r8iD5EAxVBs6TJcMDKqyH7PBuho6bBJ")
        }
        .assert_eq(&hash(&bytes).to_string());
    }

    #[test]
    fn test_account_deserialization() {
        let old_account = LegacyAccount {
            amount: 100,
            locked: 200,
            code_hash: CryptoHash::default(),
            storage_usage: 300,
        };
        let mut old_bytes = &borsh::to_vec(&old_account).unwrap()[..];
        let new_account = <Account as BorshDeserialize>::deserialize(&mut old_bytes).unwrap();
        assert_eq!(new_account.amount, old_account.amount);
        assert_eq!(new_account.locked, old_account.locked);
        assert_eq!(new_account.code_hash, old_account.code_hash);
        assert_eq!(new_account.storage_usage, old_account.storage_usage);
        assert_eq!(new_account.version, AccountVersion::V1);
        let mut new_bytes = &borsh::to_vec(&new_account).unwrap()[..];
        let deserialized_account =
            <Account as BorshDeserialize>::deserialize(&mut new_bytes).unwrap();
        assert_eq!(deserialized_account, new_account);
    }
}
