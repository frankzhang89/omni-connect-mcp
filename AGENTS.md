# Repository Instructions

## Release notes

- Every change to application code, configuration, dependencies, launchers, packaging, or tests must update `RELEASE_NOTES.md` in the same change.
- Write release notes in English. Describe user-visible behavior, fixes, or relevant maintenance changes accurately and concisely.
- Keep versions in newest-first order and preserve published release history. Record changes after the current release under an `Unreleased` section until the next release is prepared.
- When preparing a release, move the relevant unreleased entries into its version section and keep the root Maven version, module parent versions, MCP server version in `application.yml`, and README configuration example consistent.
