# Releasing to npm

This guide is for maintainers. The first release is published by hand from your computer; every later release is
published by GitHub Actions ([`.github/workflows/release.yml`](../.github/workflows/release.yml)) when you publish a
GitHub release, using npm *trusted publishing* (no npm token stored in GitHub) and *provenance* (npmjs.com shows that
the package was built from this repository by that workflow).

## Contents

- [One-time setup](#one-time-setup)
- [First release (by hand)](#first-release-by-hand)
- [Turn on trusted publishing](#turn-on-trusted-publishing)
- [Every later release](#every-later-release)
- [Add the package to the Node-RED flow library](#add-the-package-to-the-node-red-flow-library)
- [Check the published package](#check-the-published-package)
- [If something went wrong](#if-something-went-wrong)

## One-time setup

1. **npm account.** Create one at <https://www.npmjs.com/signup> and verify your e-mail address. Turn on two-factor
   authentication (*Account → Two-Factor Authentication*); npm requires it for publishing.
2. **Package name.** `package.json` uses `node-red-contrib-kafkajs-no-crash`. The Node-RED flow library refuses new
   unscoped names that start with `node-red-` without `contrib-`, so the repository name cannot be used as is. A scoped
   name such as `@your-npm-user/node-red-kafkajs-no-crash` is the alternative the Node-RED docs recommend; to use it,
   change `name` in `package.json` (and the install commands and badges in the README). `publishConfig.access` is
   already `public`, which scoped packages need. Check that the name is still free:

   ```sh
   npm view node-red-contrib-kafkajs-no-crash   # "404 Not Found" means it is free
   ```

3. **GitHub.** Merge the work into `main` (the README badges, CI and the release workflow use `main`). Keep the
   repository **public**: npm provenance is only available for packages built from public repositories.

## First release (by hand)

```sh
git clone https://github.com/patel-deepanshu/node-red-kafkajs-no-crash.git
cd node-red-kafkajs-no-crash
git checkout main
npm ci

# 1. Everything green? (CI must be green on main too, including the e2e job.)
npm run lint
npm test
npm run audit:runtime          # must report 0 vulnerabilities

# 2. Set the release date of the version in CHANGELOG.md (## [1.0.0] - YYYY-MM-DD), commit and push.

# 3. Review what will be uploaded (41 files, about 260 kB: nodes/, lib/, examples/, docs/,
#    scripts/migrate-flows.js, README.md, CHANGELOG.md, LICENSE, package.json; no tests).
npm pack --dry-run
npm publish --dry-run

# 4. Log in and publish. prepublishOnly runs lint and the whole test suite first;
#    npm asks for your two-factor code.
npm login
npm whoami
npm publish

# 5. Tag the release and push the tag.
git tag -a v1.0.0 -m "v1.0.0"
git push origin v1.0.0
```

Then create the GitHub release: *Releases → Draft a new release*, choose the tag `v1.0.0`, use the `1.0.0` section of
[CHANGELOG.md](../CHANGELOG.md) as the text, and publish it. The release workflow runs and sees that 1.0.0 is already
on npm, so it publishes nothing (its summary says so).

## Turn on trusted publishing

Once the package exists on npm:

1. On npmjs.com open the package → *Settings* → *Trusted publishing* → *GitHub Actions*, and enter:
   - organization or user: `patel-deepanshu`
   - repository: `node-red-kafkajs-no-crash`
   - workflow filename: `release.yml`
   - environment: leave empty (or create a GitHub environment, add it to the `publish` job with `environment:`, and
     enter the same name here; a GitHub environment lets you require an approval before every publish).
2. Recommended: in *Settings → Publishing access*, choose *Require two-factor authentication and disallow tokens*.
   Trusted publishing keeps working; leaked tokens can no longer publish.

No `NPM_TOKEN` secret is needed after this. (The workflow still passes `secrets.NPM_TOKEN` to npm, so a granular
access token with publish rights in that secret also works, for example if you do not want trusted publishing.
npm tries trusted publishing first.)

## Every later release

1. Make sure `main` is green in CI, including the e2e job.
2. Update the version and the changelog:

   ```sh
   npm version patch --no-git-tag-version    # or minor / major (semantic versioning, see below)
   ```

   In [CHANGELOG.md](../CHANGELOG.md), rename `[Unreleased]` to the new version with today's date, add an empty
   `[Unreleased]` section above it, and update the comparison links at the bottom.
3. Commit (`Release 1.0.1`), open a pull request, merge it into `main` once CI is green.
4. Create a GitHub release with a **new** tag `v1.0.1` on `main` (the tag must match `package.json`, the workflow
   refuses anything else) and the changelog section as its text. Publishing the release starts the workflow: it
   installs, checks the tag, runs `npm run audit:runtime` and `npm audit signatures`, runs lint and all tests
   (prepublishOnly), and publishes with provenance.
5. Check the workflow run, then the package page on npmjs.com (new version, *Provenance* section).

Which version number? Bug fixes only: patch (`1.0.1`). New options or nodes that do not change existing behaviour:
minor (`1.1.0`). Anything that can break an existing flow (a renamed or removed option, a changed default, a changed
`msg` property, a higher minimum Node.js or Node-RED version): major (`2.0.0`), and explain the migration in the
changelog. Node type names (`kafkajs-nc-*`) must never change: that breaks every flow that uses them.

## Add the package to the Node-RED flow library

The palette manager in Node-RED only lists packages that are in the flow library.

1. After the first release, go to <https://flows.nodered.org/add/node>, sign in with GitHub, and enter
   `node-red-contrib-kafkajs-no-crash`. The package needs the `node-red` keyword (it has it), a README and a
   license.
2. The package page shows a *scorecard* (naming, `node-red.version`, Node.js engines, examples, dependencies). Fix
   anything it flags in the next release.
3. The library does not notice new versions by itself: after each release, open the package page and use *request
   refresh*.

It can take a while until a new package or version appears in the palette manager's list. Until then it can be
installed by name: `cd ~/.node-red && npm install node-red-contrib-kafkajs-no-crash`, then restart Node-RED.

## Check the published package

In a clean Node-RED (for example `docker run -it -p 1880:1880 nodered/node-red`):

1. *Menu → Manage palette → Install*, search `kafkajs-no-crash`, install (or `npm install` as above).
2. *Import → Examples → node-red-contrib-kafkajs-no-crash* shows the nine example flows.
3. Deploy example 01 with a broker that is not running: the nodes must show a yellow status and keep retrying, and
   Node-RED must keep running.
4. `npx -p node-red-contrib-kafkajs-no-crash kafkajs-nc-migrate-flows --help` prints the migration tool's help.

## If something went wrong

- **A broken version was published.** Publish a fixed patch release, then mark the broken one:
  `npm deprecate node-red-contrib-kafkajs-no-crash@1.0.1 "Broken: use 1.0.2 or later"`. Users see the message on
  install.
- **Unpublishing** is possible within 72 hours (`npm unpublish node-red-contrib-kafkajs-no-crash@1.0.1`) when no
  other package depends on it, but the version number can never be used again. Prefer deprecating.
- **The release workflow failed.** Nothing was published if the failure is before the `npm publish` step. Fix the
  problem on `main`, then re-run the workflow from the *Actions* tab (it skips a version that is already on npm), or
  delete the GitHub release and tag and create them again.
- **`E404` or `ENEEDAUTH` from `npm publish` in the workflow.** Trusted publishing is not set up, or the repository,
  workflow file name or environment on npmjs.com does not match exactly.
- **A security problem was reported.** Follow [SECURITY.md](../SECURITY.md): fix it privately, publish a patch
  release, then publish a GitHub security advisory.
