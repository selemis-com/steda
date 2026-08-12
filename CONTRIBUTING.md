# Contributing Guide

Steda is developed and maintained by Selemis B.V. Contributions from the community are welcome.

## Before contributing

For bug fixes, documentation improvements, tests, and other small, well-scoped changes, feel free to open a pull request directly.

For larger changes, new features, public API changes, database schema changes, or changes to Steda's execution semantics, please open an issue first so the design can be discussed before substantial implementation work begins.

Steda aims to keep its execution model and public API small and deliberate. A useful contribution should solve a concrete problem without adding unnecessary abstraction or expanding the project's scope without a clear reason.

## Issues

If you encounter a bug, please check whether an existing issue already covers it before opening a new one.

Good bug reports include:

* a minimal reproduction where practical;
* the Steda, Rust, and PostgreSQL versions involved;
* the expected and observed behavior;
* relevant logs or error messages;
* any investigation or root-cause analysis you have already done.

Feature proposals should explain the problem being solved, the intended behavior, and why it belongs in Steda rather than in application code or a separate integration.

## Pull requests

Keep pull requests focused on a single logical change.

Please include tests for behavioral changes and update documentation when public behavior or APIs change. New functionality should use Steda's existing abstractions where possible rather than introducing parallel execution or persistence models.

Before submitting a pull request, run:

```sh
make pr
```

This runs the repository's formatting, linting, tests, examples, doctests, and other verification checks.

Pull requests may be asked to change substantially or be declined if the proposed design does not fit Steda's scope, even when the implementation itself is correct.

## Database changes

Changes to Steda's PostgreSQL schema or database behavior require particular care because persisted state must remain correct across retries, worker failures, cancellation, and upgrades.

Database changes should include appropriate migration coverage and tests for the relevant execution invariants. Avoid relying on application-side coordination where PostgreSQL can enforce the invariant directly.

Once a migration has shipped in a published Steda release, do not modify or remove it. Introduce a new migration for subsequent schema changes instead.

## Compatibility

Steda is currently pre-1.0. Breaking changes may still be made when they materially improve the API or execution model.

Even so, compatibility should not be broken casually. Pull requests that change public Rust APIs, SQL interfaces, persisted data, or documented behavior should explain why the break is worthwhile.

## Security

If you believe you have found a security vulnerability, please do not report it through GitHub Issues or a public pull request.

See our [Security Policy](SECURITY.md) for reporting instructions.

## Licensing of contributions

Steda is dual licensed under the [Apache License, Version 2.0](LICENSE-APACHE) and [MIT license](LICENSE-MIT).

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Steda, as defined in the Apache License 2.0, is licensed under
those same terms, without any additional terms or conditions.

By submitting a contribution, you represent that you have the right to submit
the contributed material under those terms.

If your contribution incorporates or is derived from third-party source
material, make that provenance clear in the pull request and preserve any
applicable license, copyright, and attribution requirements.
