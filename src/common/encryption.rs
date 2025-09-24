// Hopefully encrypt file as we receive them.

use crate::error::Error;
use aws_esdk::client as esdk_client;
use aws_esdk::material_providers::client as mpl_client;
use aws_esdk::material_providers::types::material_providers_config::MaterialProvidersConfig;
use aws_esdk::material_providers::types::{
    keyring::KeyringRef, EsdkCommitmentPolicy, PaddingScheme,
};
use aws_esdk::types::aws_encryption_sdk_config::AwsEncryptionSdkConfig;

pub struct Encryptor {
    key_ring: KeyringRef,
    esdk_client: esdk_client::Client,
    frame_size: i64,
}

impl Encryptor {
    pub async fn new(public_key: &[u8], frame_size: Option<i64>) -> Result<Encryptor, Error> {
        let esdk_config = AwsEncryptionSdkConfig::builder()
            .commitment_policy(EsdkCommitmentPolicy::RequireEncryptRequireDecrypt)
            .build()
            .map_err(|e| {
                Error::EncryptionSDKError(
                    aws_esdk::types::error::Error::AwsEncryptionSdkException {
                        message: e.to_string(),
                    },
                )
            })?;
        let esdk_client = esdk_client::Client::from_conf(esdk_config)?;
        let mpl_config = MaterialProvidersConfig::builder().build().map_err(|e| {
            Error::EncryptionSDKError(aws_esdk::types::error::Error::AwsEncryptionSdkException {
                message: e.to_string(),
            })
        })?;
        let mpl_client = mpl_client::Client::from_conf(mpl_config).map_err(|e| {
            Error::EncryptionSDKError(aws_esdk::types::error::Error::AwsEncryptionSdkException {
                message: e.to_string(),
            })
        })?;
        let key_ring = mpl_client
            .create_raw_rsa_keyring()
            .key_name("static-id")
            .key_namespace("static-random")
            .padding_scheme(PaddingScheme::OaepSha1Mgf1)
            .public_key(public_key)
            .send()
            .await
            .map_err(|e| {
                Error::EncryptionSDKError(
                    aws_esdk::types::error::Error::AwsEncryptionSdkException {
                        message: e.to_string(),
                    },
                )
            })?;
        Ok(Encryptor {
            key_ring,
            esdk_client,
            frame_size: frame_size.unwrap_or(4096),
        })
    }

    pub async fn encrypt_in_place(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(self
            .esdk_client
            .encrypt()
            .frame_length(self.frame_size)
            .plaintext(data)
            .keyring(self.key_ring.clone())
            .send()
            .await?
            .ciphertext
            .ok_or_else(|| {
                Error::EncryptionSDKError(
                    aws_esdk::types::error::Error::AwsEncryptionSdkException {
                        message: "No ciphertext returned".to_string(),
                    },
                )
            })?
            .into_inner())
    }
}

mod tests {
    #[allow(unused_imports)]
    use crate::common::encryption::Encryptor;

    #[allow(dead_code)]
    static PUBLIC_KEY: &str = r#"-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAu5FbSCf9seUwZxR6JWeU
ZNcbpKq5awFDCZORon63wx0QVOLKgfTn0T1Icj+rbVM/X8xisNMA/lC8l/UjDpyr
AP7tfUU3f0o65f3iuoxL1wkqvvVpHhArM9EeyVPX45D8XjnQBx006n/CufpGbOSB
EtN2SAOrqz0OHtmlkdt5KrC7SMlNH5lw/Vz9gW5j77n178dndzvSRPbYH10nds7p
Tg9iT9m0UrG3yqnwfHjdlEHe7RHchUG003Xda7NFuZq/yKwSG7Kmo0Fhvbj5Qwaf
A3akEAQKkUwGNoR2tpdJ5/lzpqD2btCgLoquHF0KpoCe/IqO0GpGjjRfk4/8ooAJ
IfcaFRJiuj5yq7NMYPe0FhzqDcB1KOpkIkauCe2/JW9jzJEjs/p0Al4VUUCDpEv2
c5FCcNVhMlTvx5S8yySrKsKY5t2yV2p0pyL1fg4Iwd4Typn10zclak4AiweDVr1J
Edh2sFMaJ6K8rFfOSvC3Bo4vz8aJTws8vxXmD+q3Pnj5JUjgp/ENLnYpchuKmrsr
+hCpo3A0KvbjYD9tSl0aDXJr418p7mfd0K2BLi0TaP+QsCPsU2J3tZ4s5zDVrjQj
xnkel+ZrSRaIoo6NfivJIx+fe5wBX74ZciM7rhV7e0ZmOd+zY4VdG37LrvLHk+yL
sUKpKvz8sXy1ce1Y/A/uVf8CAwEAAQ==
-----END PUBLIC KEY-----
"#;

    #[tokio::test]
    async fn test_encrypt() {
        use std::io::Write;

        let data = "Hello, world!\n".repeat(2000);
        let encryptor = Encryptor::new(PUBLIC_KEY.as_bytes(), None).await;
        assert!(encryptor.is_ok());
        let encryptor = encryptor.unwrap();
        let encrypted_data = encryptor.encrypt_in_place(data.as_bytes()).await;
        assert!(encrypted_data.is_ok());
        std::fs::File::create("/tmp/encrypted_data")
            .unwrap()
            .write_all(&encrypted_data.unwrap())
            .unwrap();
    }
}
