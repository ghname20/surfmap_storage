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
GetOptions( $cfg, qw(logpath=s capfilepath=s start|s=s end|e=s storage|d=s testdata|t select|S=s@ selectnew|n=s@
           ) );

$cfg->{storage}//='..\\db';
#$cfg->{out}//='./gg_out.json';
#$cfg->{defragment}//=1;
#my $select = $cfg->{select};
my $g = query_graph_object->new($cfg);
for my $sel(@{$cfg->{selectnew}}) {
  $g->switch_session({selection=>$sel});
}
#$g->write_db();

