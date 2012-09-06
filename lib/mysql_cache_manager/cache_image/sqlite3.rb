require 'sqlite3'

module MysqlCacheManager
  module CacheImage
    class SQLite3
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
            page_number   INTEGER NOT NULL
          )
        ",
      }

      def initialize(filename, create_if_needed)
        @filename = filename

        if File.exists?(filename)
          @db = ::SQLite3::Database.new(@filename)
        else
          if create_if_needed
            @db = ::SQLite3::Database.new(@filename)
            TABLE_SCHEMA.each do |name, schema|
              @db.query(schema)
            end
          else
            raise "File not found: #{filename}"
          end
        end

        @db.cache_size = 200000
        @db.synchronous = "off"
        @db.temp_store = "memory"
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
        @insert_page ||= @db.prepare("INSERT OR IGNORE INTO pages (space, page_number) VALUES (?, ?)")

        @insert_page.execute(space, page_number)
      end

      def each_space
        unless block_given?
          return Enumerable::Enumerator.new(self, :each_space)
        end

        spaces = 0
        @db.execute("SELECT DISTINCT space FROM pages ORDER BY space") do |row|
          spaces += 1
          yield row[0]
        end

        spaces
      end

      def each_page(space)
        unless block_given?
          return Enumerable::Enumerator.new(self, :each_page, space)
        end

        pages = 0
        @db.execute("SELECT page_number FROM pages WHERE space = #{space} ORDER BY page_number") do |row|
          pages += 1
          yield row[0]
        end

        pages
      end

      def each_page_batch(space, batch_size)
        unless block_given?
          return Enumerable::Enumerator.new(self, :each_page_batch, space, batch_size)
        end

        batch_pages = Array.new
        pages = each_page(space) do |page_number|
          batch_pages << page_number
          if batch_pages.size >= batch_size
            yield batch_pages
            batch_pages.clear
          end
        end

        unless batch_pages.empty?
          yield batch_pages
        end

        pages
      end
    end
  end
end