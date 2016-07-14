#!perl
use Test::More tests => 1;
use Test::Deep;
use lib (q{E:\work\kmg\gather});
use lib (q{J:\projects\perl\kmg\gather});
use query_graph_object;
use Data::Dumper;
use Getopt::Long qw(:config);
use Carp;

$SIG{__DIE__} = sub { Carp::confess( @_ ) };

our $cfg={};
GetOptions($cfg, qw(logpath=s capfilepath=s start|s=s end|e=s storage|d=s init|i!));

$cfg->{storage}//='J:\projects\perl\kmg\tests\data\test2\db';
$cfg->{logpath}//='J:\projects\perl\kmg\tests\data\test2\logs';


if ($cfg->{init}) {
  my $initial_core = 'J:\projects\perl\kmg\tests\data\common\query_graph_db.json';
  my $core =$cfg->{storage}."/core_graph.json";
  my $junk =$cfg->{storage}."/junk_graph.json";
  my $meta=$cfg->{storage}."/metadata_graph.json";
  unlink $core;
  unlink $junk;
  unlink $meta;
  File::Copy::copy($initial_core, $core);
}


$cfg->{select}//=[qw(none core=rw junk=rw)];

my $g = query_graph_object->new($cfg);
$g->write_db();
$cfg{defragment}//=0;
my $req = {request =>"get_node_info", start=>"20160101", end=>"20160102", url=> "chrome-extension://jhfpbldkagcaijkjcklbdccomadpedae/popup/popup.html"};
my $t = $g->get_node_info($req);
my $should  =  {
  'request' => 'get_node_info',
  'comment' => '',
  'end' => '20160102',
  'rating' => '30',
  'url' => 'chrome-extension://jhfpbldkagcaijkjcklbdccomadpedae/popup/popup.html',
  'start' => '20160101'
};
is_deeply($t, $should);
