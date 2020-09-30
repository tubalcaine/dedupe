#! /usr/bin/perl

# Written to use DBD::SQLite, but database can be overridden.

use strict;

use DBI;
use DBD::SQLite;
use Digest;
use Digest::MD5;
use File::Find;
use File::Basename;
use XML::Simple;
use XML::SAX::Expat;
use Getopt::Long;
use Tie::DBI;

# Set defaults
# File match count
my $fc = 0;

# Modulo to report progress, report every $mod files
my $mod = 3;

# Get config from XML (if present)
my $config = XMLin();

my @rejectDirs;
my @acceptDirs;

# Apply command line switches (if present)
GetOptions(
	"mod=i"      => \$mod,
	"database=s" => \$config->{database},
) or die "Command line argument error.\n";

$mod = $config->{mod} if ( defined $config->{mod} );
$config->{database} = "./filedb.sqlite" unless ( defined $config->{database} );

my $file = {};
my $hash = {};

my $dbh = DBI->connect(
	"dbi:SQLite:dbname=" . $config->{database},
	"", "",
	{
		PrintError => 1
		  ## DBI options may be placed here
	}
);

if ($DBI::err) {
	die( "Cannot open SQLite DB [" . $DBI::err . "]\n" );
}

my $res = buildDB($dbh);

## The database tables now exist for sure.
## Tie the file and hash hashes

tie %$file, "Tie::DBI", $dbh, "file", "pathname", { CLOBBER => 3 };
tie %$hash, "Tie::DBI", $dbh, "md5", "hash", { CLOBBER => 3 };

find(
	{
		no_chdir => 1,
		preprocess => sub {
			my $dirList = \@_;

			my $a_finm = $_;
			my $a_dir  = $File::Find::dir;
			my $a_name = $File::Find::name;

			my $prune = (scalar grep { /^$a_dir$/ } @{$config->{prunelist}->{prune}});

			push @rejectDirs, $a_dir if ($prune);
			push @acceptDirs, $a_dir unless ($prune);

			return (scalar grep { /^$a_dir$/ } @{$config->{prunelist}->{prune}}) ? @{[]} : @$dirList;
		},
		wanted   => sub {
			my $a_finm = $_;
			my $a_dir  = $File::Find::dir;
			my $a_name = $File::Find::name;

			return unless ( -f $File::Find::name && -r $File::Find::name );
			
			my $md5 = Digest->new("MD5");

			$fc++;
			print "Processed $fc files\r" if ( $mod && $fc % $mod == 0 );
			select()->flush() if ( $mod && $fc % $mod == 0 );

			my ( $title, $a2, $a3, $a4 ) = fileparse($_);

			open my $fh, "<$a_name";
			$md5->addfile($fh);
			close $fh;

			my $hexDigest = $md5->hexdigest;
			if (!defined $hash->{$hexDigest}) {
				# Create hash
				$hash->{$hexDigest} = { count => 1};
			} else {
				# We have a collision
				print "File $a_name appears to collide.\n";
				
				#Increment the count
				($hash->{$hexDigest}->{count})++;
			}
			
			# Create or update file
			$file->{$a_name} = { hash => $hexDigest, size => (-s $a_name) };
		}
	},
	@ARGV
);

exit 0;

sub buildDB {
	my ($dbh) = @_;

	my $query = "create table if not exists file " . "(
        pathname varchar(2048) primary key,
        hash varchar(32),
        size int
    )";

	my $res = $dbh->do($query);

	if ($DBI::err) {
		die("Failed to run query [$query]");
	}

	$query = "create table if not exists md5 " . "(
        hash varchar(32) primary key,
        count integer
    )";

	$res = $dbh->do($query);

	if ($DBI::err) {
		die("Failed to run query [$query]");
	}

	return $res;
}

