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
  end

  def empty!
    @db.query("DELETE FROM metadata")
    @db.query("DELETE FROM pages")
  end

  def add_metadata(k, v)
    @insert_metadata ||= @db.prepare("INSERT INTO metadata (k, v) VALUES (?, ?)")
    @insert_metadata.execute(k, v)
  end

  def metadata
    image_metadata = {}
    @db.execute("SELECT k, v FROM metadata ORDER BY k") do |row|
      image_metadata[row[0]] = row[1]
    end

    image_metadata
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
      yield row[0], row[1]
    end
    
    pages
  end

  def each_page_batch(batch_size)
    unless block_given?
      return Enumerable::Enumerator.new(self, :each_page_batch, batch_size)
    end

    pages = 0
    batch_space = nil
    batch_pages = []
    @db.execute("SELECT space, page_number FROM pages ORDER BY space, page_number") do |row|
      pages += 1

      if batch_pages.size >= batch_size
        yield batch_space, batch_pages
        batch_pages = []
      end

      if batch_space == row[0]
        batch_pages << row[1]
      else
        unless batch_pages.empty?
          yield batch_space, batch_pages
        end
        batch_space = row[0]
        batch_pages = [row[1]]
      end
    end

    unless batch_pages.empty?
      yield batch_space, batch_pages
    end

    pages
  end
end
