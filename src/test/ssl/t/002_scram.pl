# Test SCRAM authentication and TLS channel binding types

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

use File::Copy;

use FindBin;
use lib $FindBin::RealBin;

use SSLServer;

if ($ENV{with_openssl} ne 'yes')
{
	plan skip_all => 'SSL not supported by this build';
}

my $number_of_tests = 9;

# This is the hostname used to connect to the server.
my $SERVERHOSTADDR = '127.0.0.1';

# Allocation of base connection string shared among multiple tests.
my $common_connstr;

# Set up the server.

note "setting up data directory";
my $node = get_new_node('master');
$node->init;

# PGHOST is enforced here to set up the node, subsequent connections
# will use a dedicated connection string.
$ENV{PGHOST} = $node->host;
$ENV{PGPORT} = $node->port;
$node->start;

# Configure server for SSL connections, with password handling.
configure_test_server_for_ssl($node, $SERVERHOSTADDR, "scram-sha-256",
	"pass", "scram-sha-256");
switch_server_cert($node, 'server-cn-only');
$ENV{PGPASSWORD} = "pass";
$common_connstr =
  "dbname=trustdb sslmode=require sslcert=invalid sslrootcert=invalid hostaddr=$SERVERHOSTADDR";

# Default settings
test_connect_ok($common_connstr, "user=ssltestuser",
	"Basic SCRAM authentication with SSL");

# Test channel_binding
test_connect_fails(
	$common_connstr,
	"user=ssltestuser channel_binding=invalid_value",
	qr/invalid channel_binding value: "invalid_value"/,
	"SCRAM with SSL and channel_binding=invalid_value");
test_connect_ok(
	$common_connstr,
	"user=ssltestuser channel_binding=disable",
	"SCRAM with SSL and channel_binding=disable");
test_connect_ok(
	$common_connstr,
	"user=ssltestuser channel_binding=require",
	"SCRAM with SSL and channel_binding=require");

# Now test when the user has an MD5-encrypted password; should fail
test_connect_fails(
	$common_connstr,
	"user=md5testuser channel_binding=require",
	qr/Channel binding required but not supported by server's authentication request/,
	"MD5 with SSL and channel_binding=require");

# Now test with auth method 'cert' by connecting to 'certdb'. Should
# fail, because channel binding is not performed.
copy("ssl/client.key", "ssl/client_tmp.key");
chmod 0600, "ssl/client_tmp.key";
test_connect_fails(
	"sslcert=ssl/client.crt sslkey=ssl/client_tmp.key hostaddr=$SERVERHOSTADDR",
	"dbname=certdb user=ssltestuser channel_binding=require",
	qr/Channel binding required, but server authenticated client without channel binding/,
	"Cert authentication and channel_binding=require");

done_testing($number_of_tests);
