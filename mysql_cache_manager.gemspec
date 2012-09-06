Gem::Specification.new do |s|
  s.name        = 'mysql_cache_manager'
  s.version     = File.open("VERSION").readline.chomp
  s.date        = Time.now.strftime("%Y-%m-%d")
  s.summary     = 'MySQL Cache Manager'
  s.description = 'A tool for saving and restoring the InnoDB buffer pool using the information_schema.buffer_page table and engine_control(InnoDB, prefetch_pages, ...) function.'
  s.authors     = [ 'Jeremy Cole' ]
  s.email       = 'jeremy@jcole.us'
  s.homepage    = 'http://jcole.us/'
  s.files = [
    'lib/mysql_cache_manager.rb',
    'lib/mysql_cache_manager/cache_manager.rb',
    'lib/mysql_cache_manager/cache_image.rb',
    'lib/mysql_cache_manager/innodb_buffer_pool.rb',
  ]
  s.executables = [
    'mysql_cache_manager',
  ]
end
