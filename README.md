
## TODO

The official AWS docs suggests using the [`aws-amplify`](https://www.npmjs.com/package/aws-amplify) library.
However, it comes with a [**30MB+ install size**](https://www.npmjs.com/package/aws-amplify) and [**100+ dependencies**](https://npmgraph.js.org/?q=aws-amplify).
That's far too heavy for something as simple as a thin WebSocket client.

## Local Development

TODO: requirements: node.js 24+

To run integration tests, Terraform must be available in your `$PATH`.
If you're using Nix package manager, you can enter the development shell with:

```shell
NIXPKGS_ALLOW_UNFREE=1 nix develop . --command zsh
```

To deploy infrastructure for tests, run:

```shell
terraform init
terraform apply
```
