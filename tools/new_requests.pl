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
GetOptions( $cfg, qw(logpath=s capfilepath=s start|s=s end|e=s storage|d=s
           match_undef|U! filterfile|F=s requestfile|R=s request|r=s filters|f=s testdata|t out|o=s
           max_edges|me=s max_nodes|mn=s rescan_results|rescan select|S=s@ fiddler_include|fi=s
           ) );

#$cfg->{storage}//='..\\scratch\\db';
#$cfg->{logpath}//='J:\projects\perl\kmg\scratch\rescan\logs';
#$cfg->{out}//='./gg_out.json';
#$cfg->{defragment}//=1;
my $g = query_graph_object->new($cfg);
$g->get_new_requests();
#my $lc = $g->edge_count();
#my $nc = $g->node_count();
#printf STDERR "before deletion: n=%s e=%s\n", $nc,$lc;
#$g->delete_junk();
#$lc = $g->edge_count();
#$nc = $g->node_count();
#printf STDERR "after deletion: n=%s e=%s\n", $nc,$lc;
$g->write_db();
