package ru.fusionsoft.dbgit.integration.primitives.args;
public class ArgsCheckoutNodb extends ArgsExplicit {

    public ArgsCheckoutNodb(String branchName, String commitHash) {
        super(
            "checkout",
            branchName,
            commitHash,
            "-nodb"
        );
    }
}
