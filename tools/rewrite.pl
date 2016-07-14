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
GetOptions( $cfg, qw(logpath=s capfilepath=s start|s=s end|e=s storage|d=s testdata|t select|S=s@
           ) );

$cfg->{storage}//='..\\db';
#$cfg->{out}//='./gg_out.json';
#$cfg->{defragment}//=1;
my $g = query_graph_object->new($cfg);
#my $lc = $g->edge_count();
#my $nc = $g->node_count();
#printf STDERR "before deletion: n=%s e=%s\n", $nc,$lc;
#$g->delete_junk();
#$lc = $g->edge_count();
#$nc = $g->node_count();
#printf STDERR "after deletion: n=%s e=%s\n", $nc,$lc;
$g->changed(1);
$g->write_db();
