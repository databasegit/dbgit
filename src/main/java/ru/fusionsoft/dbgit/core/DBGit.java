package ru.fusionsoft.dbgit.core;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.eclipse.jgit.api.*;
import org.eclipse.jgit.api.ListBranchCommand.ListMode;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.api.errors.GitAPIException;
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
	private static DBGit instance;
	final private Repository repository;
	final private Git git;
	final private static int messageLevel = 0;


	private DBGit()  {
		try {
			FileRepositoryBuilder builder = new FileRepositoryBuilder();
			repository = builder
				.setGitDir(
					Paths.get("")
					.resolve(".git")
					.toAbsolutePath()
					.toFile()
				)
				.build();

			git = new Git(repository);
			instance = this;
		} catch (IOException e) {
			final String msg = "Could not build file repository, never intended to do anything after that...";
			throw new ExceptionDBGitRunTime(msg, e);
		}

	}

	private DBGit(String url) {
		try {
			//TODO find out where this 'url' is in 'repository'
			FileRepositoryBuilder builder = new FileRepositoryBuilder();
			repository = builder
				.setGitDir(new File(url))
				.build();

			git = new Git(repository);
		} catch (Exception e) {
			throw new ExceptionDBGitRunTime(e);
		}
	}

	public static DBGit getInstance() {
		if (instance == null) {

			if (!repositoryExists()) {
				final DBGitLang msg = DBGitLang.getInstance().getValue("errors", "gitRepNotFound");
				throw new ExceptionDBGitRunTime(msg);
			}

			instance = new DBGit();
		}
		return instance;
	}

	public static void initUrlInstance(String gitDirUrl, boolean force)  {
		if (instance != null) {
			if(!force) {
				final String msg = "DBGit is already initialized, btw u can set 'force' to 'true' or never make singletons...";
				throw new ExceptionDBGitRunTime(msg);
			}
			instance.repository.close();
			instance.git.close();
		}
		instance = new DBGit(gitDirUrl);
	}

	public static boolean repositoryExists() {
		if (instance == null) {
			FileRepositoryBuilder builder = new FileRepositoryBuilder();
			return builder.readEnvironment().findGitDir().getGitDir() != null;
		}
		else return true;
	}

	public Repository getRepository() {
		return repository;
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
						ConsoleWriter.println(DBGitLang.getInstance().getValue("errors", "commit", "cantFindObject"), messageLevel);
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
			ConsoleWriter.printlnGreen(DBGitLang.getInstance().getValue("general", "commit", "commit") + ": " + res.getName()
				, messageLevel
			);
			ConsoleWriter.printlnGreen(res.getAuthorIdent().getName() + "<" + res.getAuthorIdent().getEmailAddress() + ">, " + res.getAuthorIdent().getWhen()
				, messageLevel
			);

		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public void printCurrentCommit() throws IOException {
		try (RevWalk walk = new RevWalk(repository)) {
			ConsoleWriter.detailsPrintlnGreen(
				DBGitLang.getInstance().getValue("general", "checkout", "commitMessage")
					.withParams(walk.parseCommit(repository.getAllRefs().get("HEAD").getObjectId()).getShortMessage())
				, messageLevel + 1
			);
		}
	}
	
	public boolean nameInLocalRepoExistence(String name) throws IOException {
		return git.getRepository().findRef(name) != null;
	}
	
	public boolean nameInRemoteRepoExistence(String name) throws GitAPIException {
		return git.branchList().setListMode(ListMode.REMOTE).call().stream()
		.anyMatch(ref -> ref.getName().equals("refs/remotes/origin/" + name));
	}
	
	public void gitCheckoutName(String name) throws Exception {
		final CheckoutCommand checkoutCommand = git.checkout().setName(name);

		if (nameInLocalRepoExistence(name)) {
			ConsoleWriter.detailsPrintlnGreen(
				DBGitLang.getInstance().getValue("general", "checkout", "branchName").withParams(name), messageLevel + 1
			);
			if(nameInRemoteRepoExistence(name)) {
				checkoutCommand.setStartPoint("origin/" + name);
			}
		} else {
			ConsoleWriter.detailsPrintlnGreen(
				DBGitLang.getInstance().getValue("general", "checkout", "commitName").withParams(name), messageLevel + 1
			);
		}

		checkoutCommand.call();
		printCurrentCommit();
	}
	
	public void gitCheckoutNewBranch(String branchName, String commitHash) throws Exception {
		final CheckoutCommand checkoutCommand = git.checkout();
		ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "checkout", "branchName").withParams(branchName), messageLevel + 1);
		if(commitHash != null) {
			ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "checkout", "commitName").withParams(commitHash), messageLevel + 1);
		}
		ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "checkout", "toCreateBranch").withParams(String.valueOf(true)), messageLevel + 1);
		
		checkoutCommand.setCreateBranch(true);
		checkoutCommand.setName(branchName);
		if(commitHash != null) {
			if(nameInLocalRepoExistence(commitHash)) {
				checkoutCommand.setStartPoint(commitHash);
			} else {
				throw new ExceptionDBGit("Can not find the local commit specified: " + commitHash);
			}
		} else if (nameInRemoteRepoExistence(branchName)){
			checkoutCommand.setStartPoint("origin/" + branchName);
		} 
		//else {
		//	you would checkout HEAD - create new branch from current one's HEAD
		//	https://stackoverflow.com/a/37131407
		//}
		checkoutCommand.call();
	}
	
	public void gitCheckoutFileName(String fileName) throws Exception{
		MaskFilter maskAdd = new MaskFilter(fileName);
		ConsoleWriter.println("Checking out files...", messageLevel+1);

		int counter = 0;
		for (String path : getGitIndexFiles(DBGitPath.DB_GIT_PATH)) {
			if (maskAdd.match(path)) {
				git.checkout()
					.setName(git.getRepository()
					.getBranch())
					.addPath(DBGitPath.DB_GIT_PATH + "/"+ path)
					.call();
				counter++;
			}
		}
		String messageTailChars = counter != 1 ? "s" : "";
		ConsoleWriter.println(
			DBGitLang.getInstance().getValue("general", "checkout", "updatedFromIndex")
			.withParams(String.valueOf(counter), messageTailChars)
			, messageLevel+1
		);
	}
	
	public void gitCheckout(String arg1, String arg2, boolean isNewBranch) throws ExceptionDBGit {
		try {
			ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "checkout", "do"), messageLevel);
			if(isNewBranch) {
				gitCheckoutNewBranch(arg1, arg2);
			} else {
//				if(nameInRemoteRepoExistence(arg1) || nameInLocalRepoExistence(arg1)) {
					gitCheckoutName(arg1);
//				} else {
//					gitCheckoutFileName(arg1);
//				}
			}

		} catch (Exception e) {
			throw new ExceptionDBGit("Failed to do checkout", e);
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
			ConsoleWriter.println(DBGitLang.getInstance()
				.getValue("errors", "pull", "emptyGitRepository")
				, 0
			);
			//throw new ExceptionDBGit(e);
		}
	}

	public void gitPush(String remoteName) throws ExceptionDBGit {
		try {
			git.log().call();
		} catch (Exception e) {
			ConsoleWriter.println(DBGitLang.getInstance()
			    .getValue("general", "push", "noCommitsFound")
			    , messageLevel
			);
			return;
		}

		try {
			ConsoleWriter.println(DBGitLang.getInstance()
			    .getValue("general", "push", "remoteName")
			    .withParams((remoteName.equals("") ? Constants.DEFAULT_REMOTE_NAME : remoteName))
			    , messageLevel+1
			);

			try {

				Iterable<PushResult> result = git
					.push()
					.setCredentialsProvider(
						getCredentialsProviderByName(
							remoteName.equals("")
								? Constants.DEFAULT_REMOTE_NAME
								: remoteName
						)
					)
					.setRemote(
						remoteName.equals("")
							? Constants.DEFAULT_REMOTE_NAME
							: remoteName
					)
					.call();


				ConsoleWriter.println(DBGitLang.getInstance()
					.getValue("general", "push", "called")
					, messageLevel+1
				);
	
				result.forEach(pushResult -> {
					if (pushResult == null){
						ConsoleWriter.println(DBGitLang.getInstance()
							.getValue("general", "push", "nullResult")
							, 1
						);
					} else {
						ConsoleWriter.println(DBGitLang.getInstance()
							.getValue("general", "push", "callResult")
							.withParams(pushResult.toString())
							, messageLevel+1
						);
					}
					for (RemoteRefUpdate res : pushResult.getRemoteUpdates()) {
						if (res.getStatus() == RemoteRefUpdate.Status.UP_TO_DATE){
							ConsoleWriter.println(DBGitLang.getInstance()
								.getValue("general", "push", "upToDate")
								, 1
							);
						}
						else {
							ConsoleWriter.println(DBGitLang.getInstance()
								.getValue("general", "push", "result")
								.withParams(res.toString())
								, 1
							);
						}
					}
				});

			} catch (Exception ex) {
				ConsoleWriter.println(DBGitLang.getInstance()
						.getValue("general", "push", "callResult")
						.withParams(ex.getLocalizedMessage())
					, messageLevel + 1
				);
			}

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

			ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "init", "created"), messageLevel);

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

			final Git call = cc.call();
			call.getRepository().close();
			call.close();

			ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "clone", "cloned"), messageLevel);

		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}

	}

	public void gitRemote(String command, String name, String uri) throws ExceptionDBGit {
		try {
			switch (command) {
				case "" : {
					git.remoteList().call().forEach(remote -> ConsoleWriter.println(remote.getName(), messageLevel+1));
					break;
				}

				case "add" : {
					RemoteAddCommand remote = git.remoteAdd();
					remote.setName(name);
					remote.setUri(new URIish(uri));
					remote.call();
					remote.getRepository().close();

					ConsoleWriter.printlnGreen(DBGitLang.getInstance().getValue("general", "remote", "added"), messageLevel);

					break;
				}

				case "remove" : {
					RemoteRemoveCommand remote = git.remoteRemove();
					remote.setName(name);
					remote.call();
					remote.getRepository().close();

					ConsoleWriter.printlnGreen(DBGitLang.getInstance().getValue("general", "remote", "removed"), messageLevel);

					break;
				}

				default : ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "remote", "unknown"), messageLevel);
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
			ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "done"), messageLevel);
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public void gitFetch(String remote) throws ExceptionDBGit {
		try {
			FetchCommand fetch = git
			.fetch()
			.setCredentialsProvider(getCredentialsProviderByName(
				remote.equals("") 
					? Constants.DEFAULT_REMOTE_NAME 
					: remote
				)
			);

			if (remote.length() > 0)
				fetch = fetch.setRemote(remote);
			else
				fetch = fetch.setRemote(Constants.DEFAULT_REMOTE_NAME);

			fetch.call();
			fetch.getRepository().close();

			ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "done"), messageLevel);
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	private CredentialsProvider getCredentialsProviderByName(String remoteName) throws ExceptionDBGit {
		ConsoleWriter.detailsPrintln(DBGitLang.getInstance()
		    .getValue("general", "gettingRepoLink")
		    , messageLevel
		);
		String link = git.getRepository().getConfig().getString("remote", remoteName, "url");
		ConsoleWriter.detailsPrintln(DBGitLang.getInstance()
			.getValue("general", "repoLink")
			.withParams(link)
			, messageLevel
		);

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

			URIish uri = new URIish(link);

			ConsoleWriter.println(DBGitLang.getInstance()
				.getValue("general", "showDbCredentials")
				.withParams( uri.getScheme(), uri.getUser(), uri.getHost(), uri.getRawPath())
				, 0
			);

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
				ConsoleWriter.detailsPrintlnRed(DBGitLang.getInstance().getValue("errors", "gitLoginNotFound"), messageLevel);
				return null;
			}
			return new UsernamePasswordCredentialsProvider(uri.getUser(), uri.getPass());
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

}
