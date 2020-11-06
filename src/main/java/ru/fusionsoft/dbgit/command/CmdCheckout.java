package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLine.Builder;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevWalk;
import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import java.text.MessageFormat;

public class CmdCheckout implements IDBGitCommand {
	
	private Options opts = new Options();

	public CmdCheckout() {
		opts.addOption("b", false, getLang().getValue("help", "checkout-b").toString());
		opts.addOption("r", false, getLang().getValue("help", "checkout-r").toString());
		opts.addOption("u", false, getLang().getValue("help", "checkout-u").toString());
		opts.addOption("nodb", false, getLang().getValue("help", "checkout-no-db").toString());
		opts.addOption("upgrade", false, getLang().getValue("help", "checkout-u").toString());
		opts.addOption("ls", false, getLang().getValue("help", "checkout-ls").toString());
	}
	
	@Override
	public String getCommandName() {
		return "checkout";
	}

	@Override
	public String getParams() {
		return "<branch> <commit>";
	}

	@Override
	public String getHelperInfo() {
		return getLang().getValue("help", "checkout").toString();
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		
		String[] args = cmdLine.getArgs();
		ConsoleWriter.setDetailedLog(cmdLine.hasOption("v"));

		Repository repo =  DBGit.getInstance().getRepository();
		Ref head = repo.getAllRefs().get("HEAD");

		if (cmdLine.hasOption("ls")){
			try(RevWalk walk = new RevWalk(repo)){
				String branch = repo.getBranch();
				String headNumber = head.getObjectId().getName();
				String headName = head.getName();
				String message = walk.parseCommit(head.getObjectId()).getShortMessage();

				ConsoleWriter.printlnGreen(MessageFormat.format(
					"{0} ({1}) {2}",
					!branch.equals(headNumber) ? branch + ": " + headName : headNumber,
					headName,
					message
				));
			}
			return;
		}

		if (!cmdLine.hasOption("u") && !cmdLine.hasOption("nodb"))
			checkVersion();
		if (args == null || args.length == 0) {
			throw new ExceptionDBGit(getLang().getValue("errors", "checkout", "badCommand"));		
		} else if (args.length == 1) {
			DBGit.getInstance().gitCheckout(args[0], null, cmdLine.hasOption("b"));
		} else if (args.length == 2) {
			DBGit.getInstance().gitCheckout(args[0], args[1], cmdLine.hasOption("b"));
		}		
		
		Builder builder = new CommandLine.Builder();

		if (cmdLine.hasOption("u")) {
			CmdDump dumpCommand = new CmdDump();
			builder.addOption(new Option("u", false, ""));

			if (cmdLine.hasOption("v")) {
				builder.addOption(new Option("v", false, ""));			
			}
			
			dumpCommand.execute(builder.build());
		} else if (!cmdLine.hasOption("nodb")) {			
			CmdRestore restoreCommand = new CmdRestore();
			
			if (cmdLine.hasOption("r")) {
				builder.addOption(new Option("r", false, ""));			
			}
			if (cmdLine.hasOption("v")) {
				builder.addOption(new Option("v", false, ""));			
			}
			if (cmdLine.hasOption("s")) {
				Option scriptOption = new Option("s", true, "");
				scriptOption.getValuesList().add(cmdLine.getOptionValue("s"));
				builder.addOption(scriptOption);
			}
			
			restoreCommand.execute(builder.build());
		}

	}

}
