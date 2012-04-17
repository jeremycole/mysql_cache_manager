require 'sqlite3'

class CacheImage
  TABLE_SCHEMA = {
    "metadata" => "
      CREATE TABLE metadata (
        k STRING NOT NULL,
        v STRING NOT NULL,
        PRIMARY KEY (k)
      )
    ",
    "pages" => "
      CREATE TABLE pages (
        space         INTEGER NOT NULL,
        page_number   INTEGER NOT NULL,
        PRIMARY KEY (space, page_number)
      )
    ",
  }

  def initialize(filename, create_if_needed)
    @filename = filename

    if File.exists?(filename)
      @db = SQLite3::Database.new(@filename)
    else
      if create_if_needed
        @db = SQLite3::Database.new(@filename)
        TABLE_SCHEMA.each do |name, schema|
          @db.query(schema)
        end
      else
        raise "File not found: #{filename}"
      end
    end

    @db.results_as_hash = true
  end

  def empty!
    @db.query("DELETE FROM metadata")
    @db.query("DELETE FROM pages")
  end

  def add_metadata(k, v)
    @insert_metadata ||= @db.prepare("INSERT INTO metadata (k, v) VALUES (?, ?)")
    @insert_metadata.execute(k, v)
  end

  def get_metadata
    metadata = {}
    @db.execute("SELECT k, v FROM metadata ORDER BY k") do |row|
      metadata[k] = v
    end

    metadata
  end

  def save_pages(page_iterator)
    @db.transaction
    pages = page_iterator.each do |page|
      save_page(*page)
    end
    @db.commit
    
    pages
  end

  def save_page(space, page_number)
    @insert_page ||= @db.prepare("INSERT INTO pages (space, page_number) VALUES (?, ?)")

    @insert_page.execute(space, page_number)
  end

  def each_page
    unless block_given?
      return Enumerable::Enumerator.new(self, :each_page)
    end

    pages = 0
    @db.execute("SELECT space, page_number FROM pages ORDER BY space, page_number") do |row|
      pages += 1
      yield row["space"], row["page_number"]
    end
    
    pages
  end
end