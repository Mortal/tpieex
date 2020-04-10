#include <tpie/tpie.h>
#include <tpie/tpie_log.h>
#include <tpie/sort.h>

struct Item {
	unsigned long long a, b, c;
};

// Example of creating a compressed stream.
// Remove "compression_normal" to create an uncompressed stream instead.
void create_it(const char * filename) {
	tpie::file_stream<Item> xs;
	xs.open(filename, tpie::open::write_only | tpie::open::compression_normal);
	xs.write(Item{9,8,7});
	xs.write(Item{3,2,1});
	xs.write(Item{6,5,4});
}

// Example of scanning through a file_stream.
void dump_it(const char * filename) {
	tpie::file_stream<Item> xs;
	xs.open(filename, tpie::open::read_only);
	// Skip first item - not available because of compression_normal:
	// xs.seek(1);
	while (xs.can_read()) {
		auto item = xs.read();
		tpie::log_info() << "Read item: " << item.a << " " << item.b << " " << item.c << std::endl;
	}
}

// Example of using tpie::sort to sort data that sits in a file_stream.
void sort_it(const char * filename) {
	tpie::file_stream<Item> xs;
	xs.open(filename);
	tpie::progress_indicator_null pi;
	tpie::sort(xs, [&](const Item & a, const Item & b) -> bool { return a.a < b.a; }, pi);
}

// Example of using seek, can_read_back and read_back to read a stream backwards.
void dump_it_backwards(const char * filename) {
	tpie::file_stream<Item> xs;
	xs.open(filename, tpie::open::read_only);
	xs.seek(0, tpie::file_stream_base::end);
	while (xs.can_read_back()) {
		auto item = xs.read_back();
		tpie::log_info() << "Read item reverse: " << item.a << " " << item.b << " " << item.c << std::endl;
	}
}

// Example of using get_position and set_position to seek in a compressed stream.
void seek_stream_position(const char * filename) {
	tpie::file_stream<Item> xs;
	xs.open(filename, tpie::open::read_only);
	std::vector<tpie::stream_position> positions;
	positions.push_back(xs.get_position());
	while (xs.can_read()) {
		xs.read();
		positions.push_back(xs.get_position());
	}
	xs.set_position(positions[1]);
	auto item = xs.read();
	tpie::log_info() << "Random seek compressed: " << item.a << " " << item.b << " " << item.c << std::endl;
}

// Example of using tpie::merge_sorter to sort data that is generated on the fly
// instead of sitting in a file_stream.
void streaming_sort() {
	auto pred = [&](const Item & a, const Item & b) -> bool { return a.a < b.a; };
	tpie::merge_sorter<Item, true, decltype(pred)> sorter(pred);
	sorter.set_available_memory(tpie::get_memory_manager().available());
	tpie::progress_indicator_null pi;
	sorter.begin();
	sorter.push(Item{9,8,7});
	sorter.push(Item{3,2,1});
	sorter.push(Item{6,5,4});
	sorter.end();
	sorter.calc(pi);
	while (sorter.can_pull()) {
		auto item = sorter.pull();
		tpie::log_info() << "Pull item from sorter: " << item.a << " " << item.b << " " << item.c << std::endl;
	}
}

int main() {
	tpie::tpie_init();

	size_t available_memory_mb = 128;
	tpie::get_memory_manager().set_limit(available_memory_mb*1024*1024);

	tpie::log_info() << "Hello world!" << std::endl;

	create_it("the_file.tpie");
	dump_it("the_file.tpie");
	sort_it("the_file.tpie");
	dump_it("the_file.tpie");
	dump_it_backwards("the_file.tpie");
	seek_stream_position("the_file.tpie");

	streaming_sort();

	tpie::tpie_finish();
	return 0;
}
