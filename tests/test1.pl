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

$cfg->{storage}//='J:\projects\perl\kmg\tests\data\test1\db';
$cfg->{logpath}//='J:\projects\perl\kmg\tests\data\test1\logs';

if ($cfg->{init}) {
  my $initial_core = 'J:\projects\perl\kmg\tests\data\test1\db\test4db.json';
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
$cfg{defragment}//=0;
  if ($g->get_new_requests()) {
    $g->write_db();
  }
#my $req = {request =>"get_node_info", url=> "chrome-extension://jhfpbldkagcaijkjcklbdccomadpedae/popup/popup.html"};
my $req = {request =>"get_node_info", key=> "google:windows+10+latest+build"};
my $t = $g->get_node_info($req);
my $results = {
  'request' => 'get_node_info',
  'comment' => '',
  #'url' => 'chrome-extension://jhfpbldkagcaijkjcklbdccomadpedae/popup/popup.html',
  #'query'=>'windows+10+latest+build',
  'rating' => '40',
  key=> "google:windows+10+latest+build"
};
is_deeply($t, $results);
