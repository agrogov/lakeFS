package azure_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/benburkert/dns"
	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/pkg/block/azure"
)

const (
	azuriteContainerTimeoutSeconds = 10 * 60 // 10 min
	containerName                  = "container1"
	accountName                    = "account1"
	accountKey                     = "key1"
	domain                         = azure.BlobEndpointTestDomain // TLD for test
)

var (
	pool              *dockertest.Pool
	blockURL          string
	dockerUnavailable error
	azuriteStartError error
)

func createDNSResolver() {
	zone := dns.Zone{
		Origin: domain + ".",
		TTL:    5 * time.Minute,
		RRs: dns.RRSet{
			accountName: map[dns.Type][]dns.Record{
				dns.TypeA: {
					&dns.A{A: net.IPv4(127, 0, 0, 1).To4()},
				},
			},
		},
	}

	mux := new(dns.ResolveMux)
	mux.Handle(dns.TypeANY, zone.Origin, &zone)
	client := &dns.Client{
		Resolver: mux,
	}
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial:     client.Dial,
	}
}

func runAzurite(dockerPool *dockertest.Pool) (string, func(), error) {
	ctx := context.Background()
	// The azblob SDK negotiates a newer REST API version than Azurite supports,
	// so run Azurite with --skipApiVersionCheck to accept the request. The default
	// Cmd is overridden, so the host bindings from the image are restated here.
	resource, err := dockerPool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/azure-storage/azurite",
		Tag:        "3.35.0",
		Env: []string{
			fmt.Sprintf("AZURITE_ACCOUNTS=%s:%s", accountName, accountKey),
		},
		Cmd: []string{
			"azurite", "-l", "/data",
			"--blobHost", "0.0.0.0",
			"--queueHost", "0.0.0.0",
			"--tableHost", "0.0.0.0",
			"--skipApiVersionCheck",
		},
	})
	if err != nil {
		return "", nil, fmt.Errorf("start Azurite: %w", err)
	}

	accountHost := accountName + "." + domain
	createDNSResolver()

	// set cleanup
	closer := func() {
		if err := dockerPool.Purge(resource); err != nil {
			log.Printf("could not purge Azurite container: %v", err)
		}
	}

	// expire, just to make sure
	err = resource.Expire(azuriteContainerTimeoutSeconds)
	if err != nil {
		closer()
		return "", nil, fmt.Errorf("expire Azurite container: %w", err)
	}

	// create connection and test container
	port := resource.GetPort("10000/tcp")
	url := fmt.Sprintf("http://%s:%s", accountHost, port)
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		closer()
		return "", nil, fmt.Errorf("build Azurite credential: %w", err)
	}

	blob, err := azblob.NewClientWithSharedKeyCredential(url, cred, nil)
	if err != nil {
		closer()
		return "", nil, fmt.Errorf("build Azurite client: %w", err)
	}
	_, err = blob.CreateContainer(ctx, containerName, nil)
	if err != nil {
		closer()
		return "", nil, fmt.Errorf("create Azurite container: %w", err)
	}

	// return container URL
	return url, closer, nil
}

// requireAzurite skips a test that needs the emulator when Docker was not reachable in TestMain.
// The package's pure tests still run on a machine without Docker.
func requireAzurite(t *testing.T) {
	t.Helper()
	if dockerUnavailable != nil {
		t.Skipf("the Azurite integration test needs Docker: %v", dockerUnavailable)
	}
	if azuriteStartError != nil {
		t.Fatalf("Docker is reachable but Azurite did not start: %v", azuriteStartError)
	}
}

// TestMain starts the Azurite container once. A machine without a reachable Docker daemon does not
// fail the package: the error is recorded and the tests that need the emulator skip through
// requireAzurite. On colima the daemon is not at Docker's default socket, so run with
// DOCKER_HOST=unix://$HOME/.colima/default/docker.sock.
func TestMain(m *testing.M) {
	var err error
	pool, err = dockertest.NewPool("")
	if err == nil {
		err = pool.Client.Ping()
	}
	var cleanup func()
	if err != nil {
		dockerUnavailable = fmt.Errorf("connect to Docker: %w", err)
	} else {
		blockURL, cleanup, err = runAzurite(pool)
		if err != nil {
			azuriteStartError = err
		}
	}
	code := m.Run()
	if cleanup != nil {
		cleanup()
	}
	os.Exit(code)
}

// TestDockerUnavailableSkips runs the package's Azurite test in a child test binary whose
// DOCKER_HOST cannot exist. Before the fix TestMain panicked there; it must now skip instead.
func TestDockerUnavailableSkips(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("locate the test binary: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestAzureAdapter$", "-test.v=true")
	env := make([]string, 0, len(os.Environ())+1)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "DOCKER_HOST=") {
			env = append(env, entry)
		}
	}
	cmd.Env = append(env, "DOCKER_HOST=unix://"+filepath.Join(os.TempDir(), "lakefs-az7-no-such-docker.sock"))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the child test binary must exit 0 when Docker is unreachable, not fail or panic: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "--- SKIP: TestAzureAdapter") {
		t.Fatalf("the Azurite test must skip when Docker is unreachable:\n%s", output)
	}
	if strings.Contains(string(output), "panic:") {
		t.Fatalf("TestMain must not panic when Docker is unreachable:\n%s", output)
	}
}
