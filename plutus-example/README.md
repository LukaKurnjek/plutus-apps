# plutus-example

This library demonstrates end to end examples of creating and executing Plutus scripts on chain.

This is done roughly in the following steps:

1. Write your Plutus **on chain** code.
2. Serialize your Plutus on chain code to the text envelope format (`cardano-cli` expects this format).
3. Create your transaction with the accompanying Plutus script(s).
4. Submit transaction to execute Plutus script.

## FAQ

### Where is the off chain code?

The off chain code is used for transaction construction. In this case we construct the transaction with `cardano-cli` and therefore we don't need to write any off chain code.

### Where can I learn about Plutus scripts in more detail?

Our education director, Lars Brünjes, has an excellent series of [tutorials](https://www.youtube.com/@iogacademy/playlists?view=50&sort=dd&shelf_id=2) on youtube. You can also check out the GitHub page of the [Plutus Pioneer Program](https://github.com/input-output-hk/plutus-pioneer-program/). Further relevant Plutus documentation can be found [here](https://docs.cardano.org/plutus/learn-about-plutus).


