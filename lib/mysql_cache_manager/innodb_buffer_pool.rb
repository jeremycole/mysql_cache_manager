module MysqlCacheManager
  class InnodbBufferPool
    QUERY_STATUS = <<-EOQ
      SELECT * FROM information_schema.global_status
      WHERE variable_name LIKE 'INNODB\\_%'
    EOQ

    QUERY_PAGES = <<-EOQ
      SELECT space, page_number
      FROM information_schema.innodb_buffer_page
      WHERE page_type IN ("INDEX")
    EOQ

    QUERY_PAGES_FAST = <<-EOQ
      SELECT space, page_number
      FROM information_schema.innodb_buffer_page_basic
    EOQ

    def initialize(mysql)
      @mysql = mysql
    end

    def status
      status_result = @mysql.query(QUERY_STATUS)
      status_vars = {}
      status_result.each_hash do |row|
        var = row['VARIABLE_NAME'].sub("INNODB_", "").downcase
        status_vars[var] = row['VARIABLE_VALUE'].to_i
      end
      status_vars
    end

    def each_page
      unless block_given?
        return Enumerable::Enumerator.new(self, :each_page)
      end

      # Determine if the current server supports the "new" dump method
      @mysql.select_db('INFORMATION_SCHEMA')

      @mysql.list_tables.each do |table|
        if table == "INNODB_BUFFER_PAGE_BASIC"
          QUERY_PAGES = QUERY_PAGES_FAST
        end
      end

      pages = 0
      pages_result = @mysql.query(QUERY_PAGES)
      pages_result.each_hash do |row|
        pages += 1
        if 0 == (pages % 1000)
          puts "Fetched #{pages} pages so far..."
        end
        yield row["space"].to_i, row["page_number"].to_i
      end

      pages
    end

    def fetch_page(space, pages)
      unless pages.is_a? Array
        pages = [pages]
      end
      fetch_query = "SELECT engine_control(innodb, prefetch_pages, #{space}, #{pages.join(',')}) AS pages_fetched"
      if result = @mysql.query(fetch_query)
        if status_row = result.fetch_hash
          return status_row["pages_fetched"].to_i
        end
      end
      nil
    end
  end
end