package ru.fusionsoft.dbgit.core;

import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.diogonunes.jcdp.color.api.Ansi;
import org.eclipse.jgit.api.*;
import org.eclipse.jgit.api.ListBranchCommand.ListMode;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.PushResult;
import org.eclipse.jgit.transport.RemoteRefUpdate;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.MaskFilter;

public class DBGit {
	private static DBGit dbGit = null;
	private Repository repository;
	private Git git;

	private DBGit() throws ExceptionDBGit {
		try {
			FileRepositoryBuilder builder = new FileRepositoryBuilder();
			repository = builder
					.readEnvironment() // scan environment GIT_* variables
					.findGitDir() // scan up the file system tree
					.build();

			git = new Git(repository);
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	private DBGit(String url) throws ExceptionDBGit {
		try {
			FileRepositoryBuilder builder = new FileRepositoryBuilder();
			repository = builder
					.setGitDir(new File(url))
					.build();

			git = new Git(repository);
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public static DBGit getInstance() throws ExceptionDBGit {
		if (dbGit == null) {
			FileRepositoryBuilder builder = new FileRepositoryBuilder();

			if (builder.readEnvironment().findGitDir().getGitDir() == null) {
				throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "gitRepNotFound"));
			}

			dbGit = new DBGit();
		}
		return dbGit;
	}
	public static DBGit initUrlInstance(String gitDirUrl, boolean force) throws ExceptionDBGit {
		if (dbGit != null && !force) {
			throw new ExceptionDBGit("Already initialized");
		}

		dbGit = new DBGit(gitDirUrl);
		return dbGit;
	}

	public static boolean checkIfRepositoryExists() throws ExceptionDBGit {
		if (dbGit == null) {
			FileRepositoryBuilder builder = new FileRepositoryBuilder();

			if (builder.readEnvironment().findGitDir().getGitDir() == null) {
				return false;
			} else {
				return true;
			}

		} else
			return true;
	}

	public Repository getRepository() {
		return repository;
	}

	public void setRepository(Repository repository) {
		this.repository = repository;
	}

	public String getRootDirectory() {
		return repository.getDirectory().getParent();
	}

	/**
	 * Get list git index files by path.
	 *
	 * @param path
	 * @return
	 */
	public List<String> getGitIndexFiles(String path) throws ExceptionDBGit {
		try {
			DirCache cache = repository.readDirCache();
			List<String> files = new ArrayList<String>();
			Integer pathLen = path.length();
			if (!(path.endsWith("/") || path.endsWith("\\") || path.equals(""))) {
				pathLen++;
			}

			for (int i = 0; i < cache.getEntryCount(); i++) {
				String file = cache.getEntry(i).getPathString();

				//System.out.rintln(cache.getEntry(i).getPathString() +"   "+cache.getEntry(i).getObjectId().getName());


				if (file.startsWith(path)) {
					files.add(file.substring(pathLen));
				}
			}

			return files;
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public List<String> getAddedObjects(String path) throws ExceptionDBGit {
		try {
			List<String> files = new ArrayList<String>();
			Integer pathLen = path.length();
			if (!(path.endsWith("/") || path.endsWith("\\") || path.equals(""))) {
				pathLen++;
			}

			Status st = git.status().call();
			for (String file : st.getAdded()) {
				if (file.startsWith(path)) {
					files.add(file.substring(pathLen));
				}
			}

			return files;
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public void addFileToIndexGit(String filename) throws ExceptionDBGit {
		//https://github.com/centic9/jgit-cookbook/blob/master/src/main/java/org/dstadler/jgit/porcelain/AddFile.java
		try {
        	/*
        	System.out.rintln(repository.getBranch());
        	System.out.rintln(filename);
        	 */
			git.add().addFilepattern(filename).call();
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public void removeFileFromIndexGit(String filename) throws ExceptionDBGit {
		try {
			git.rm().addFilepattern(filename).call();
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public Set<String> getModifiedFiles() throws ExceptionDBGit {
		try {
			return git.status().call().getModified();
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public Set<String> getChanged() throws ExceptionDBGit {
		try {
			return git.status().call().getChanged();
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public void gitCommit(boolean existsSwitchA, String msg, String path) throws ExceptionDBGit {
		try {
			if (existsSwitchA) {
				GitMetaDataManager gmdm = GitMetaDataManager.getInstance();
				IMapMetaObject fileObjs = gmdm.loadFileMetaData();
				DBGitIndex index = DBGitIndex.getInctance();

				for (IMetaObject obj : fileObjs.values()) {
					String hash = obj.getHash();
					if (!gmdm.loadFromDB(obj)) {
						ConsoleWriter.println(DBGitLang.getInstance().getValue("errors", "commit", "cantFindObject"));
						obj.removeFromGit();
						index.markItemToDelete(obj);
						index.saveDBIndex();
						index.addToGit();

						continue;
					}

					if (!obj.getHash().equals(hash)) {
						obj.saveToFile();
						index.addItem(obj);
						obj.addToGit();
					}
				}

				index.saveDBIndex();
				index.addToGit();
			}

			RevCommit res;
			if (path == null || path.length() == 0) {
				if (msg.length() > 0 ) {
					res = git.commit().setAll(existsSwitchA).setMessage(msg).call();
				} else {
					res = git.commit().setAll(existsSwitchA).call();
				}
			} else {
				if (msg.length() > 0 ) {
					res = git.commit().setAll(existsSwitchA).setOnly(DBGitPath.DB_GIT_PATH + "/" + path).setMessage(msg).call();
				} else {
					res = git.commit().setAll(existsSwitchA).setOnly(DBGitPath.DB_GIT_PATH + "/" + path).call();
				}
			}
			ConsoleWriter.printlnGreen(DBGitLang.getInstance().getValue("general", "commit", "commit") + ": " + res.getName());
			ConsoleWriter.printlnGreen(res.getAuthorIdent().getName() + "<" + res.getAuthorIdent().getEmailAddress() + ">, " + res.getAuthorIdent().getWhen());

		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public void gitCheckout(String branch, String commit, boolean isNewBranch) throws ExceptionDBGit {
		try {
			ConsoleWriter.detailsPrintLn(DBGitLang.getInstance().getValue("general", "checkout", "do"));
			ConsoleWriter.detailsPrintLnColor(DBGitLang.getInstance().getValue("general", "checkout", "toCreateBranch") + ": " + isNewBranch, 1, Ansi.FColor.GREEN);
			ConsoleWriter.detailsPrintLnColor(DBGitLang.getInstance().getValue("general", "checkout", "branchName") + ": " + branch, 1, Ansi.FColor.GREEN);
			if (commit != null)
				ConsoleWriter.detailsPrintLnColor(DBGitLang.getInstance().getValue("general", "checkout", "commitName") + ": " + commit, 1, Ansi.FColor.GREEN);



			Ref result;
			if (git.getRepository().findRef(branch) != null || isNewBranch) {

				CheckoutCommand checkout = git.checkout().setCreateBranch(isNewBranch).setName(branch);

				if (commit != null){
					checkout = checkout.setName(commit);
				} else {
					if (git.branchList().setListMode(ListMode.REMOTE).call().stream()
						.filter(ref -> ref.getName().equals("refs/remotes/origin/" + branch))
						.count() > 0
					)checkout = checkout.setStartPoint("remotes/origin/" + branch);
				}

				result = checkout.call();

				try(RevWalk walk = new RevWalk(repository)){
					ConsoleWriter.detailsPrintLnColor(MessageFormat.format("{0}: {1}"
						, DBGitLang.getInstance().getValue("general", "checkout", "commitMessage")
						, walk.parseCommit(repository.getAllRefs().get("HEAD").getObjectId()).getShortMessage()
					), 1, Ansi.FColor.GREEN);
				}


			} else {
				MaskFilter maskAdd = new MaskFilter(branch);

				int counter = 0;
				for (String path: getGitIndexFiles(DBGitPath.DB_GIT_PATH)) {
					if (maskAdd.match(path)) {
						result = git.checkout().setName(git.getRepository().getBranch()).addPath(DBGitPath.DB_GIT_PATH + "/" + path).call();
						counter++;
					}
				}
				String s = "";
				if (counter != 1) s = "s";
				ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "checkout", "updatedFromIndex").withParams(String.valueOf(counter), s), 1);
			}


		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public void gitMerge(Set<String> branches) throws ExceptionDBGit {
		try {
			MergeCommand merge = git.merge();

			for (String branch : branches) {
				merge = merge.include(git.getRepository().findRef(branch));
			}

			MergeResult result = merge.call();

			ConsoleWriter.println(result.getMergeStatus().toString(), 1);

		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public void gitPull(String remote, String remoteBranch) throws ExceptionDBGit {
		try {
			PullCommand pull = git.pull();
			if (remote.length() > 0)
				pull = pull.setRemote(remote);
			else
				pull = pull.setRemote(Constants.DEFAULT_REMOTE_NAME);

			if (remoteBranch.length() > 0)
				pull = pull.setRemoteBranchName(remoteBranch);
			ConsoleWriter.printlnGreen(
				pull.setCredentialsProvider(getCredentialsProviderByName(pull.getRemote())).call()
				.toString()
				, 1
			);
		} catch (Exception e) {
			ConsoleWriter.println("Repo is empty!");
			//throw new ExceptionDBGit(e);
		}
	}

	public void gitPush(String remoteName) throws ExceptionDBGit {
		try {
			git.log().call();
		} catch (Exception e) {
			ConsoleWriter.println("No commits found!");
			return;
		}

		try {
			ConsoleWriter.detailsPrintLn("Entered to gitPush");
			ConsoleWriter.detailsPrintLn("remoteName: " + (remoteName.equals("") ? Constants.DEFAULT_REMOTE_NAME : remoteName));

			Iterable<PushResult> result = git.push()
					.setCredentialsProvider(getCredentialsProviderByName(remoteName.equals("") ? Constants.DEFAULT_REMOTE_NAME : remoteName))
					.setRemote(remoteName.equals("") ? Constants.DEFAULT_REMOTE_NAME : remoteName).call();

			ConsoleWriter.detailsPrintLn("Push called ");

			result.forEach(pushResult -> {
				if (pushResult == null)
					ConsoleWriter.detailsPrintLn("Push result is null!!! ");
				pushResult.toString();
				for (RemoteRefUpdate res : pushResult.getRemoteUpdates()) {
					if (res.getStatus() == RemoteRefUpdate.Status.UP_TO_DATE)
						ConsoleWriter.println("Everything up-to-date");
					else {
						ConsoleWriter.println(res.toString());
					}
				}
			});

		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public static void gitInit(String dirPath) throws ExceptionDBGit {
		try {
			InitCommand init = Git.init();

			if (!dirPath.equals("")) {
				File dir = new File(dirPath);
				if (!dir.exists()) {
					throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "dirNotFound"));
				}
				init.setDirectory(dir);
			}

			init.call();

			ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "init", "created"));

		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public static void gitClone(String link, String remoteName, File directory) throws ExceptionDBGit {
		try {
			String actualRemoteName = remoteName.equals("") ? Constants.DEFAULT_REMOTE_NAME : remoteName;
			CredentialsProvider cp = getCredentialsProvider(link);
			CloneCommand cc = Git.cloneRepository()
				.setURI(link)
				.setRemote(actualRemoteName)
				.setCredentialsProvider(cp)
				.setDirectory(directory);

			cc.call();

			ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "clone", "cloned"));

		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}

	}

	public void gitRemote(String command, String name, String uri) throws ExceptionDBGit {
		try {
			switch (command) {
				case "" : {
					git.remoteList().call().forEach(remote -> ConsoleWriter.println(remote.getName()));
					break;
				}

				case "add" : {
					RemoteAddCommand remote = git.remoteAdd();
					remote.setName(name);
					remote.setUri(new URIish(uri));
					remote.call();

					ConsoleWriter.printlnGreen(DBGitLang.getInstance().getValue("general", "remote", "added"));

					break;
				}

				case "remove" : {
					RemoteRemoveCommand remote = git.remoteRemove();
					remote.setName(name);
					remote.call();

					ConsoleWriter.printlnGreen(DBGitLang.getInstance().getValue("general", "remote", "removed"));

					break;
				}

				default : ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "remote", "unknown"));
			}

		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public void gitReset(String mode) throws ExceptionDBGit {
		try {
			if (mode == null)
				git.reset().call();
			else
				git.reset().setMode(ResetType.valueOf(mode)).call();
			ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "done"));
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public void gitFetch(String remote) throws ExceptionDBGit {
		try {
			FetchCommand fetch = git.fetch()
					.setCredentialsProvider(getCredentialsProviderByName(remote.equals("") ? Constants.DEFAULT_REMOTE_NAME : remote));

			if (remote.length() > 0)
				fetch = fetch.setRemote(remote);
			else
				fetch = fetch.setRemote(Constants.DEFAULT_REMOTE_NAME);

			fetch.call();

			ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "done"));
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	private CredentialsProvider getCredentialsProviderByName(String remoteName) throws ExceptionDBGit {
		ConsoleWriter.detailsPrintLn("Getting link to repo... ");
		String link = git.getRepository().getConfig().getString("remote", remoteName, "url");
		ConsoleWriter.detailsPrintLn("link:" + link);

		if (link != null)
			return getCredentialsProvider(link);
		else
			throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "gitRemoteNotFound").withParams(remoteName));
	}

	private CredentialsProvider getCredentialsProvider() throws ExceptionDBGit {

		return getCredentialsProvider(git.getRepository().getConfig().getString("remote", Constants.DEFAULT_REMOTE_NAME, "url"));
	}

	private static CredentialsProvider getCredentialsProvider(String link) throws ExceptionDBGit {
		try {
			ConsoleWriter.detailsPrintLn("Getting credentials...");

			URIish uri = new URIish(link);

			ConsoleWriter.detailsPrintLn("uri login = " + uri.getUser());
			ConsoleWriter.detailsPrintLn("uri pass = " + uri.getPass());
			/*
			Pattern patternPass = Pattern.compile("(?<=:(?!\\/))(.*?)(?=@)");
			Pattern patternLogin = Pattern.compile("(?<=\\/\\/)(.*?)(?=:(?!\\/))");
			
			String login = "";
			String pass = "";

			Matcher matcher = patternPass.matcher(link);
			if (matcher.find())
			{
				pass = matcher.group();				
			} else {
				throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "gitPasswordNotFound"));
			}
			
			matcher = patternLogin.matcher(link);
			if (matcher.find())
			{
				login = matcher.group();				
			} else {
				throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "gitLoginNotFound"));
			}
			
			ConsoleWriter.detailsPrintLn("login: " + login);
			ConsoleWriter.detailsPrintLn("pass: " + pass);*/
			if(uri.getUser() == null) {
				ConsoleWriter.detailsPrintlnRed(DBGitLang.getInstance().getValue("errors", "gitLoginNotFound"));
				return null;
			}
			return new UsernamePasswordCredentialsProvider(uri.getUser(), uri.getPass());
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

}
