# 1 Billion Row Challenge - BEAM edition

This is an attempt at solving the [1 Billion Row Challenge](https://github.com/gunnarmorling/1brc) using Elixir and Erlang. The spirit of this challenge is to process a text file with 1 billion lines using just what the languages offer in their standard libraries (i.e. no libraries).

The current best solution in this repository runs the 1BRC in ~85 seconds on a Desktop with an AMD Ryzen 7800X runnning Windows 11 (inside WSL2). I am sure that there are still lots of improvements to be made and I wouldn't be surprised if we could get the execution time down to less than 30 seconds.

## Dependencies

### Java 21 and input file

Make sure before cloning the project that you've installed Java 21 (e.g. OpenJDK 21) and generated the ~13GB `measurements.txt` file [using these instructions](https://github.com/gunnarmorling/1brc?tab=readme-ov-file#running-the-challenge)

### mix dependencies

The project dependencies are only for generating metrics and graphs from the execution. Use `mix deps.get` after cloning to fetch the dependencies.

## Running

Use the following commands to build and run the project:

- Building the executable:
```
mix escript.build
```

- Running the experiment:
```
./brc PATH_TO_MEASUREMENTS_FILE
```

### Testing out new implementations

You may clone this repository and add another implementation by creating a module in the `lib/brc/implementation` folder.
See `Brc.Implementation.Alpha` for inspiration.
When adding a new implementation, use the `Brc.Implementation` behavior and implement the `run/1` callback: by returning
a map of the results, the main module `Brc` will print them out according to the format specified by the challenge creator.

After creating a new implementation (e.g. `Brc.Implementation.Bravo`), run the executable with the `-m Brc.Implementation.Bravo` argument.

### Changing parameters

The main executable supports a `-p <integer>` argument which stores in `Application.env` the specified number of worker processes to be used.
Specific implementations may read from `Application.env` to easily add support for dynamic number of worker processes.

## Contributing

Feel free to contribute by proposing a new solution in a new separate module and [open a pull request](https://github.com/goncalotomas/1brc-ex/pulls).