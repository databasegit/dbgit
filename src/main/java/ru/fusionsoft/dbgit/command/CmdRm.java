package ru.fusionsoft.dbgit.command;

import java.sql.Timestamp;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.*;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaObjectFactory;
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.MaskFilter;

public class CmdRm implements IDBGitCommand {
	private Options opts = new Options();

	public CmdRm() {
		opts.addOption("db", false, getLang().getValue("help", "rm-db").toString());
		opts.addOption("idx", false, getLang().getValue("help", "rm-idx").toString());
	}

	public String getCommandName() {
		return "rm";
	}

	public String getParams() {
		return "<object>";
	}

	public String getHelperInfo() {
		return getLang().getValue("help", "rm").toString();
	}

	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		if (cmdLine.getArgs().length == 0) {
			throw new ExceptionDBGit(getLang().getValue("errors", "rm", "badCommand"));
		}

		checkVersion();

		ConsoleWriter.setDetailedLog(cmdLine.hasOption("v"));
		boolean forgetImmediately = cmdLine.hasOption("idx") && !cmdLine.hasOption("db");

		String nameObj = cmdLine.getArgs()[0];
		MaskFilter maskAdd = new MaskFilter(nameObj);

		DBGitIndex index = DBGitIndex.getInctance();

		ConsoleWriter.detailsPrintln(getLang().getValue("general", "rm", "checking"), messageLevel+1);

		GitMetaDataManager gmdm = GitMetaDataManager.getInstance();
		IMapMetaObject dbObjs = gmdm.loadFileMetaDataForce();
		dbObjs.putAll(gmdm.loadDBMetaData());

		IMapMetaObject deleteObjs = new TreeMapMetaObject();

		Integer countDelete = 0;

		ConsoleWriter.detailsPrintln(getLang().getValue("general", "rm", "deleting"), messageLevel+1);
		for (ItemIndex idxItem : index.getTreeItems().values()) {
			if (maskAdd.match(idxItem.getName())) {
				Timestamp timestampBefore = new Timestamp(System.currentTimeMillis());
				ConsoleWriter.detailsPrintln(getLang().getValue("general", "rm", "processing")
					.withParams(idxItem.getName())
					, messageLevel+1
				);
				ConsoleWriter.detailsPrintln(getLang().getValue("general", "rm", "removingFromGit"), messageLevel+2);

				IMetaObject metaObject = dbObjs.get(idxItem.getName());
				if(metaObject != null){
					deleteObjs.put(metaObject);
					countDelete += metaObject.removeFromGit();
				} else {
					countDelete += gmdm.removeFromGit(idxItem);
					metaObject = IMetaObject.create(idxItem.getName());
				}

				ConsoleWriter.detailsPrintGreen(getLang().getValue("general", "ok"));
				ConsoleWriter.detailsPrintln(getLang().getValue("general", "rm", "markingToDelete") + " ... ", messageLevel+2);
				index.markItemToDelete(metaObject);
				ConsoleWriter.detailsPrintGreen(getLang().getValue("general", "ok"));


				Timestamp timestampAfter = new Timestamp(System.currentTimeMillis());
				Long diff = timestampAfter.getTime() - timestampBefore.getTime();
				ConsoleWriter.detailsPrint(getLang().getValue("general", "time").withParams(diff.toString()));
			}
		}
		if (forgetImmediately) {
			ConsoleWriter.detailsPrintln(getLang().getValue("general", "rm", "removingFromIndex"), messageLevel+1);
			index.getTreeItems().values()
				.stream()
				.filter(ItemIndex::getIsDelete)
				.collect(Collectors.toList())
				.forEach(
					x->index.getTreeItems()
					.remove(x.getName())
				);
			
			index.saveDBIndex();
			index.addToGit();
			ConsoleWriter.detailsPrintGreen(getLang().getValue("general", "ok"));
		}

		if (cmdLine.hasOption("db")) {
			ConsoleWriter.detailsPrintln(getLang().getValue("general", "rm", "removingFromDb"), messageLevel+1);
			gmdm.deleteDataBase(deleteObjs, true);
			ConsoleWriter.detailsPrintGreen(getLang().getValue("general", "ok"));
		}

		if (countDelete > 0) {
			index.saveDBIndex();
			index.addToGit();
		} else {
			ConsoleWriter.printlnRed(getLang().getValue("errors", "rm", "cantFindFile").withParams(nameObj), messageLevel+1);
		}
		ConsoleWriter.println(getLang().getValue("general", "done"), messageLevel);
	}


}
