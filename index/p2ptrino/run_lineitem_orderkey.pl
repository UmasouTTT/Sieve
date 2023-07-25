#!/usr/bin/perl

use strict;
use warnings;
use File::Basename;
use File::Path;
use List::Util;
use Time::HiRes qw/time/;

# GLOBALS
my $SCRIPT_NAME = basename( __FILE__ );
my $SCRIPT_PATH = dirname( __FILE__ );

open(LINEITEM_ORDERKEY_QUERY_LOG, ">>/proj/dst-PG0/p2pevaluate/logs/lineitem_orderkey.log") or die "can not open lineitem_orderkey.log";
chdir $SCRIPT_PATH;
chdir 'sqls';
my $query = 'lineitem_orderkey.sql';

print "query : $query start \n";
open(SQLCONTENT, "<$query");
my @lines = <SQLCONTENT>;
print LINEITEM_ORDERKEY_QUERY_LOG "$lines[0]";
my $queryStart = time();
my $cmd="(/mydata/bigdata/trino-server-370/bin/trino --server localhost:8080 --catalog hive -f $query)";
my @warnoutput=`$cmd`;
my $queryEnd = time();
my $queryTime = $queryEnd - $queryStart;
print LINEITEM_ORDERKEY_QUERY_LOG "$query:\n";
print LINEITEM_ORDERKEY_QUERY_LOG "time:$queryTime\n";
print LINEITEM_ORDERKEY_QUERY_LOG "result:@warnoutput\n";

close LINEITEM_ORDERKEY_QUERY_LOG;