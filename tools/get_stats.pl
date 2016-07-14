#!perl
use strict;

use lib (q{E:\work\kmg\gather});
use lib (q{J:\projects\perl\kmg\gather});
use query_graph_object;
use Data::Dumper;
use Getopt::Long qw(:config no_ignore_case);
use Carp;
$SIG{__DIE__} = sub { Carp::confess( @_ ) };
our $cfg = {};
GetOptions( $cfg, qw(logpath=s capfilepath=s start|s=s end|e=s storage|d=s select|S=s@
           ) );

$cfg->{storage}//='..\\db';
#$cfg->{out}//='./gg_out.json';
#$cfg->{defragment}//=1;
my $g = query_graph_object->new($cfg);
my $stats = $g ->get_stats();
print Data::Dumper->new([$stats])->Terse(1)->Sortkeys(1)->Dump();
