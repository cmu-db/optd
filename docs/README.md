# Development Documentation

The `src` folder contains the documentation on the optd query optimizer in the `mdBook` format.


To view the documentation locally, you can follow the [`mdBook` installation guide](https://rust-lang.github.io/mdBook/guide/installation.html) to set up the environment. After installing `mdBook`, run the following command from the root of the optd repository:

```shell
mdbook serve --open docs/
```

If you want to edit or add a chapter to the book, start from [SUMMARY.md](./src/SUMMARY.md) which lists a table of contents. For more information, please check out the [mdBook documentation](https://rust-lang.github.io/mdBook/format/index.html).
