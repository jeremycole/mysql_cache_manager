require 'pstore'

module MysqlCacheManager
  module CacheImage
    class Pstore

      def initialize(filename, create_if_needed)
        @filename = filename

        if File.exists?(filename)
          @store = PStore.new(@filename)
        else
          if create_if_needed
            @store = PStore.new(@filename)
            @store.transaction do
              @store[:metadata] = Hash.new
              @store[:pages] = Hash.new
            end
          else
            raise "File not found: #{filename}"
          end
        end
      end

      def empty!
        @store.transaction do
          @store[:metadata] = Hash.new
          @store[:pages] = Hash.new
        end
      end

      def add_metadata(k, v)
        @store.transaction do
          @store[:metadata][k] = v
        end
      end

      def metadata
        @store[:metadata]
      end

      def save_pages(page_iterator)
        pages = 0
        @store.transaction do
          pages = page_iterator.each do |page|
            save_page(*page)
          end
        end

        pages
      end

      def save_page(space, page_number)
        @store[:pages][space] = Array.new if @store[:pages][space].nil?
        @store[:pages][space] << page_number
      end

      def each_space
        unless block_given?
          return Enumerable::Enumerator.new(self, :each_space)
        end
        @store.transaction do
          @store[:pages].keys.sort.each do |space|
            yield space
          end
        end
        @store[:pages].length
      end

      def each_page(space)
        unless block_given?
          return Enumerable::Enumerator.new(self, :each_page, space)
        end

        @store[:pages][space].sort.each do |page|
          yield page
        end

        @store[:pages][space].length
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

      def add_index
      end
    end
  end
end
