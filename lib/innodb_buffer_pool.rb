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

    pages = 0
    pages_result = @mysql.query(QUERY_PAGES)
    pages_result.each_hash do |row|
      pages += 1
      yield row["space"].to_i, row["page_number"].to_i
    end
    
    pages
  end

  def fetch_page(space, pages)
    unless pages.is_a? Array
      pages = [pages]
    end
    fetch_query = "SELECT engine_control(innodb, prefetch_pages, #{space}, #{pages.join(',')}) AS status"
    #puts "Query: #{fetch_query}"
    # SELECT ENGINE_CONTROL(InnoDB, prefetch_pages, 0, 1);
    if result = @mysql.query(fetch_query)
      if status_row = result.fetch_hash
        return status_row["status"].to_i
      end
    end
    nil
  end

end