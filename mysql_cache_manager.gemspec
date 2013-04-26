lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)
require "mysql_cache_manager/version"

Gem::Specification.new do |s|
  s.name        = 'mysql_cache_manager'
  s.version     = MysqlCacheManager::VERSION
  s.date        = Time.now.strftime("%Y-%m-%d")
  s.summary     = 'MySQL Cache Manager'
  s.description = 'A tool for saving and restoring the InnoDB buffer pool using the information_schema.buffer_page table and engine_control(InnoDB, prefetch_pages, ...) function.'
  s.authors     = [ 'Jeremy Cole', 'Will Gunty', 'Anton Kuraev' ]
  s.email       = [ 'jeremy@jcole.us', 'wg@twitter.com', 'akuraev@twitter.com' ]
  s.homepage    = 'http://jcole.us/'
  s.files = [
    'lib/mysql_cache_manager.rb',
    'lib/mysql_cache_manager/cache_manager.rb',
    'lib/mysql_cache_manager/cache_image.rb',
    'lib/mysql_cache_manager/cache_image/sqlite3.rb',
    'lib/mysql_cache_manager/cache_image/pstore.rb',
    'lib/mysql_cache_manager/innodb_buffer_pool.rb',
    'lib/mysql_cache_manager/version.rb',
  ]
  s.executables = [
    'mysql_cache_manager',
  ]
  s.add_dependency('inifile', '>=2.0.2')
end