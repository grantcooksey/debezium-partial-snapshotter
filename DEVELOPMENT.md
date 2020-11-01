# Development

Sometimes it is helpful to work with a local debezium dependency snapshots if debezium changes need to be tested.
Set `env=dev` in the `gradle.properties` file and verify that the local snapshot version matches `devDebeziumVersion`
in `build.gradle`.

## Releasing

Make sure to set the deployment secrets for gradle. See `gradle.properties`.

Verify the project is correct for the new release. Currently, version updating is done manually.
Upload a staging release to Sonatype by running `gradle publish` and close and release the staging
repository at [Sonatype](https://oss.sonatype.org).
