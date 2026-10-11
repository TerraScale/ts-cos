package cos

import (
	"context"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/secrets"
	"github.com/oracle/oci-go-sdk/v65/vault"
)

// ts-cos secrets live in ts-ops-vault (the only kept OCI resources, user 2026-10-11); production vaults are never used.
const (
	vaultRegion      = "sa-saopaulo-1"
	vaultCompartment = "ocid1.compartment.oc1..aaaaaaaazb4rfjdb3bfi2rdlle3smjfhcvuf5rq57qk3q3cyytdgc6anlkca"
	vaultID          = "ocid1.vault.oc1.sa-saopaulo-1.ffvmxfwxaad5y.abtxeljranepvnbymucmhphlo5r47qpmzntw2hivduv6owoj4cumtsdsntnq"
	vaultKeyID       = "ocid1.key.oc1.sa-saopaulo-1.ffvmxfwxaad5y.abtxeljrerrhjb2xbpqi5ptjd4swjgh5mhlfub2wfohri3nzbhnhxxvw33ea"
	vaultMasterName  = "cos-master"
)

var errVaultMissing = errors.New("OCI Vault secret is missing")

type vaultAPI interface {
	read(ctx context.Context, name string) ([]byte, error)
	create(ctx context.Context, name string, content []byte) error
}

// ociVault signs requests with the host's ~/.oci/config DEFAULT API key.
type ociVault struct {
	secrets secrets.SecretsClient
	vaults  vault.VaultsClient
}

func newOCIVault() (*ociVault, error) {
	provider := common.DefaultConfigProvider()
	s, err := secrets.NewSecretsClientWithConfigurationProvider(provider)
	if err != nil {
		return nil, errors.New("OCI Vault client unavailable; check ~/.oci/config")
	}
	v, err := vault.NewVaultsClientWithConfigurationProvider(provider)
	if err != nil {
		return nil, errors.New("OCI Vault client unavailable; check ~/.oci/config")
	}
	s.SetRegion(vaultRegion)
	v.SetRegion(vaultRegion)
	return &ociVault{secrets: s, vaults: v}, nil
}

func vaultStatus(err error) string {
	if failure, ok := common.IsServiceError(err); ok {
		return fmt.Sprintf("HTTP %d %s", failure.GetHTTPStatusCode(), failure.GetCode())
	}
	return "request failed"
}

func (o *ociVault) read(ctx context.Context, name string) ([]byte, error) {
	response, err := o.secrets.GetSecretBundleByName(ctx, secrets.GetSecretBundleByNameRequest{
		SecretName: &name, VaultId: new(vaultID), Stage: secrets.GetSecretBundleByNameStageCurrent,
	})
	if failure, ok := common.IsServiceError(err); ok && failure.GetHTTPStatusCode() == http.StatusNotFound {
		return nil, errVaultMissing
	}
	if err != nil {
		return nil, fmt.Errorf("OCI Vault read of %s: %s", name, vaultStatus(err))
	}
	content, ok := response.SecretBundleContent.(secrets.Base64SecretBundleContentDetails)
	if !ok || content.Content == nil {
		return nil, fmt.Errorf("OCI Vault secret %s has no base64 content", name)
	}
	decoded, err := base64.StdEncoding.DecodeString(*content.Content)
	if err != nil {
		return nil, fmt.Errorf("OCI Vault secret %s is not base64", name)
	}
	return decoded, nil
}

func (o *ociVault) create(ctx context.Context, name string, content []byte) error {
	_, err := o.vaults.CreateSecret(ctx, vault.CreateSecretRequest{CreateSecretDetails: vault.CreateSecretDetails{
		CompartmentId: new(vaultCompartment), VaultId: new(vaultID), KeyId: new(vaultKeyID), SecretName: &name,
		Description: new("Created automatically by COS"),
		SecretContent: vault.Base64SecretContentDetails{
			Content: new(base64.StdEncoding.EncodeToString(content)), Stage: vault.SecretContentDetailsStageCurrent,
		},
	}})
	if err != nil {
		return fmt.Errorf("OCI Vault create of %s: %s", name, vaultStatus(err))
	}
	return nil
}

// vaultMaster reads the cluster master secret, creating it on the first boot of any node.
// Concurrent first boots race on the unique secret name; losers read the winner's value.
func vaultMaster(ctx context.Context, api vaultAPI) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	created := false
	var createErr error
	for {
		master, err := api.read(ctx, vaultMasterName)
		if err == nil {
			if len(master) != 32 {
				return nil, errors.New("OCI Vault cos-master must hold 32 bytes")
			}
			return master, nil
		}
		if !errors.Is(err, errVaultMissing) {
			return nil, err
		}
		if !created {
			master = make([]byte, 32)
			rand.Read(master)
			// OCI answers 409 or 400 while another node's secret is still being created.
			createErr = api.create(ctx, vaultMasterName, master)
			clear(master)
			created = true
		}
		// A new secret version is readable a few seconds after creation.
		select {
		case <-ctx.Done():
			if createErr != nil {
				return nil, createErr
			}
			return nil, errors.New("OCI Vault cos-master did not become readable")
		case <-time.After(2 * time.Second):
		}
	}
}

// derive returns a hex credential for one purpose; every node derives the same value.
func derive(master []byte, purpose string) string {
	key, err := hkdf.Key(sha256.New, master, nil, "cos "+purpose, 32)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(key)
}
