require 'mysql'
require 'innodb_buffer_pool'
require 'cache_image'

class MysqlCacheManager
  attr_accessor :mysql, :innodb_buffer_pool, :image

  def initialize(host, user, password)
    @mysql_host = host
    @mysql_user = user
    @mysql_password = password
    @image = nil

    connect
  end

  def connect
    @mysql = Mysql.new(@mysql_host, @mysql_user, @mysql_password)
    @innodb_buffer_pool = InnodbBufferPool.new(@mysql)
  end
  
  def save_cache(filename)
    @image = CacheImage.new(filename, true)
    @image.empty!

    @innodb_buffer_pool.status.each do |k, v|
      if ["total", "data", "misc", "free"].include? k
        @image.add_metadata("innodb.pages_#{k}", v)
      end
    end

    @image.save_pages(@innodb_buffer_pool.each_page)
  end

  def restore_cache(filename, batch_size=100)
    @image = CacheImage.new(filename, false)

    pages_by_space = @image.each_page.inject({}) do |result, page|
      (result[page[0]] ||= []) << page[1]
      result
    end

    pages_attempted = 0
    pages_fetched = 0
    pages_by_space.each do |space, page_list|
      page_list.each_slice(batch_size) do |page_batch|
        pages_attempted += page_batch.size
        pages_fetched += @innodb_buffer_pool.fetch_page(space, page_batch)
        if block_given?
          yield @mysql, pages_fetched, pages_attempted
        end
      end
    end
    pages_fetched
  end
end