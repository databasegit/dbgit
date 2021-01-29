package ru.fusionsoft.dbgit.command;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVPrinter;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.DBGitIgnore;
import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.data_table.RowData;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.meta.MetaTableData;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.MaskFilter;

public class CmdAdd implements IDBGitCommand {

	private Options opts = new Options();

	public CmdAdd() {
		opts.addOption("c", false, getLang().getValue("help", "add-c").toString());
	}

	public String getCommandName() {
		return "add";
	}

	public String getParams() {
		return "<file_mask>";
	}

	public String getHelperInfo() {
		return getLang().getValue("help", "add").toString();
	}

	public Options getOptions() {
		return opts;
	}

	public void execute(CommandLine cmdLine)  throws Exception {
		if (cmdLine.getArgs().length == 0) {
			throw new ExceptionDBGit(getLang().getValue("errors", "add", "badCommand"));
		}

		checkVersion();
		ConsoleWriter.setDetailedLog(cmdLine.hasOption("v"));

		String nameObj = cmdLine.getArgs()[0];
		MaskFilter maskAdd = new MaskFilter(nameObj);

		DBGitIndex index = DBGitIndex.getInctance();

		GitMetaDataManager gmdm = GitMetaDataManager.getInstance();
		IMapMetaObject dbObjs = gmdm.loadDBMetaData();

		Integer countSave = 0;
		for (IMetaObject obj : dbObjs.values()) {
			if (cmdLine.hasOption("c")
					&& !obj.getName().equalsIgnoreCase(DBGitConfig.getInstance().getString("core", "CURRENT_OBJECT", ""))
					&& !DBGitConfig.getInstance().getString("core", "CURRENT_OBJECT", "").equals("") ) {

				continue;
			}

			if ((maskAdd.match(obj.getName()) && !obj.getName().contains(".csv"))) {

				DBGitConfig.getInstance().setValue("CURRENT_OBJECT", obj.getName().replace(".csv", ".tbl"));

				Timestamp timestampBefore = new Timestamp(System.currentTimeMillis());
				ConsoleWriter.detailsPrintln(getLang().getValue("general", "add", "processingObject")
					.withParams(obj.getName())
					, messageLevel
				);
				ConsoleWriter.detailsPrintln(getLang().getValue("general", "add", "savingToFile"), messageLevel+1);

				//TODO
				obj.saveToFile();

				ConsoleWriter.detailsPrintGreen(getLang().getValue("general", "ok"));
				ConsoleWriter.detailsPrintln(getLang().getValue("general", "addToGit"), messageLevel+1);
				countSave += obj.addToGit();
				ConsoleWriter.detailsPrintGreen(getLang().getValue("general", "ok"));

				Timestamp timestampAfter = new Timestamp(System.currentTimeMillis());
				Long diff = timestampAfter.getTime() - timestampBefore.getTime();
				ConsoleWriter.detailsPrint(getLang().getValue("general", "time").withParams(diff.toString()));

				index.addItem(obj);

				DBGitIgnore ignore = DBGitIgnore.getInstance();

				if (obj instanceof MetaTable && maskAdd.match(obj.getName().replace(".tbl", ".csv"))
						&& !ignore.matchOne(obj.getName().replace(".tbl", ".csv"))) {
					MetaTableData tblData = new MetaTableData(((MetaTable) obj).getTable());
					tblData.setName(obj.getName().replace(".tbl", ".csv"));
					File file = new File(DBGitPath.getFullPath(null)+"/"+obj.getFileName().replace(".tbl", ".csv"));
					DBGitPath.createDir(file.getParent());

					FileOutputStream out = new FileOutputStream(file.getAbsolutePath());
					CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(out), tblData.getCSVFormat());

					gmdm.setCurrentPortion(DBGitConfig.getInstance().getInteger("core", "CURRENT_PORTION", 0));
					boolean isFirstPortion = true;

					if (cmdLine.hasOption("c"))
						isFirstPortion = (DBGitConfig.getInstance().getInteger("core", "CURRENT_PORTION", 0) == 0);

					while (gmdm.loadNextPortion((MetaTable) obj)) {
						ConsoleWriter.detailsPrintln(getLang().getValue("general", "add", "writing").toString(), messageLevel+1);
						try {
							//gmdm.getCurrent().serialize(out);
							Integer count = 0;
							List<String> fields = null;

							for (RowData rd : gmdm.getCurrent().getmapRows().values()) {
								if (count == 0 && isFirstPortion) {
									//fields = rd.getData().keySet();
									fields = gmdm.getCurrent().getFields();
									csvPrinter.printRecord(fields);
									isFirstPortion = false;
								}

								rd.saveDataToCsv(csvPrinter, tblData.getTable());

								count++;
							}


						} catch (Exception e) {
							throw new ExceptionDBGit(e);
						}
					}

					csvPrinter.close();
					out.close();

					tblData.addToGit();
					index.addItem(tblData);
				}
			}
		}

		if (countSave > 0) {
			index.saveDBIndex();
			index.addToGit();
		} else {
			ConsoleWriter.printlnRed(getLang().getValue("errors", "add", "cantFindObjectInDb").withParams(nameObj), messageLevel);
		}
		ConsoleWriter.println(getLang().getValue("general", "done"), messageLevel);
	}
}
