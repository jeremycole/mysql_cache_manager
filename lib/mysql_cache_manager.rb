require 'mysql'
require 'innodb_buffer_pool'
require 'cache_image'

class MysqlCacheManager
  USEFUL_METADATA = [
    "buffer_pool_pages_total",
    "buffer_pool_pages_data",
    "buffer_pool_pages_misc",
    "buffer_pool_pages_free"
  ]

  attr_accessor :mysql, :innodb_buffer_pool, :image, :timing

  def initialize(host, user, password)
    @mysql_host = host
    @mysql_user = user
    @mysql_password = password
    @image = nil
    @timing = Hash.new(0.0)

    connect
  end

  def track_timing(name)
    raise "No block given" unless block_given?
    start_time = Time.now.to_f
    yield
    end_time = Time.now.to_f

    @timing[name] += (end_time - start_time)
  end

  def connect
    @mysql = Mysql.new(@mysql_host, @mysql_user, @mysql_password)
    @innodb_buffer_pool = InnodbBufferPool.new(@mysql)
  end
  
  def save_cache(filename)
    track_timing("open") do
      @image = CacheImage.new(filename, true)
      @image.empty!
    end

    track_timing("stats") do
      @innodb_buffer_pool.status.each do |k, v|
        if USEFUL_METADATA.include? k
          @image.add_metadata(k, v)
        end
      end
    end

    track_timing("save") do
      @image.save_pages(@innodb_buffer_pool.each_page)
    end
  end

  def restore_cache(filename, batch_size=100)
    track_timing("open") do
      @image = CacheImage.new(filename, false)
    end

    pages_attempted = 0
    pages_fetched = 0
    @image.each_space do |space|
      @image.each_page_batch(space, batch_size) do |page_batch|
        track_timing("fetch") do
          pages_attempted += page_batch.size
          pages_fetched += @innodb_buffer_pool.fetch_page(space, page_batch)
        end

        if block_given?
          track_timing("yield") do
            yield @mysql, pages_fetched, pages_attempted
          end
        end
      end
    end
    pages_fetched
  end
end