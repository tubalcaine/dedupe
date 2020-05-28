#! /usr/bin/perl

# Written to use DBD::SQLite, but database can be overridden.

use strict;

use DBI;
use DBD::SQLite;
use Digest;
use File::Find;
use File::Basename;
use XML::Simple;
use Getopt::Long;
use Tie::DBI;

# Set defaults
# File match count
my $fc = 0;
# Modulo to report progress, report every $mod files
my $mod = 3;

# Get config from XML (if present)
my $config = XMLin();
$mod = $config->{mod} if (defined $config->{mod});

# Apply command line switches (if present)


find(
	{
		no_chdir => 1,
		wanted   => sub {
			my $a_finm = $_;
			my $a_dir = $File::Find::dir;
			my $a_name = $File::Find::name;
			
			return unless ( -f $File::Find::name && -r $File::Find::name );
			
			print "Calaulating $a_finm...\n";
			
			my $md5 = Digest->new("MD5");

			$fc++;
			print "Processed $fc files\r" if ($mod && $fc % $mod == 0);
			select()->flush() if ($mod && $fc % $mod == 0);
			
			my ( $title, $a2, $a3, $a4 ) = fileparse($_);

			open my $fh, "<$a_name";
			$md5->addfile($fh);
			close $fh;
			
			my $hexDigest = $md5->hexdigest;
			print "[$hexDigest]\t$a_finm\n";
		}
	},
	@ARGV
);

exit 0;

