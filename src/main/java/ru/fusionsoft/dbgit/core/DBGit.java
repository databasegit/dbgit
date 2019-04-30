package ru.fusionsoft.dbgit.core;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.InitCommand;
import org.eclipse.jgit.api.MergeCommand;
import org.eclipse.jgit.api.MergeResult;
import org.eclipse.jgit.api.PullCommand;
import org.eclipse.jgit.api.RemoteAddCommand;
import org.eclipse.jgit.api.RemoteRemoveCommand;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
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
	
	public static DBGit getInstance() throws ExceptionDBGit {
		if (dbGit == null) {
			FileRepositoryBuilder builder = new FileRepositoryBuilder();
			
			if (builder.readEnvironment().findGitDir().getGitDir() == null) {
				ConsoleWriter.printlnRed( "Git repository not found");				
				System.exit(0);
			}
			
			dbGit = new DBGit();
		}
		return dbGit;
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
				GitMetaDataManager gmdm = GitMetaDataManager.getInctance();
				IMapMetaObject fileObjs = gmdm.loadFileMetaData();
				DBGitIndex index = DBGitIndex.getInctance();

				for (IMetaObject obj : fileObjs.values()) {
					String hash = obj.getHash();
					if (!gmdm.loadFromDB(obj)) {
						ConsoleWriter.println("Can't find " + obj.getName() + ", it will be removed from index");
						obj.removeFromGit();
						index.deleteItem(obj);
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
			ConsoleWriter.printlnGreen("commit: " + res.getName());
			ConsoleWriter.printlnGreen(res.getAuthorIdent().getName() + "<" + res.getAuthorIdent().getEmailAddress() + ">, " + res.getAuthorIdent().getWhen());
			
        } catch (Exception e) {
        	throw new ExceptionDBGit(e);
        } 
	}
	
	public void gitCheckout(String branch, String commit, boolean isNewBranch) throws ExceptionDBGit {
		try {
			ConsoleWriter.detailsPrintLn("To create new branch: " + isNewBranch);
			ConsoleWriter.detailsPrintLn("Branch name: " + branch);
			ConsoleWriter.detailsPrintLn("Commit name: " + commit);

			Ref result;
			if (git.getRepository().findRef(branch) != null || isNewBranch) {
				
				result = git.checkout().setCreateBranch(isNewBranch).setName(branch).setStartPoint(commit).call();

				ConsoleWriter.printlnGreen(result.getName());
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
				ConsoleWriter.println("Updated " + counter + " path" + s + " from the index");
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
			
			ConsoleWriter.println(result.getMergeStatus().toString());
			
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
			ConsoleWriter.printlnGreen(pull.setCredentialsProvider(getCredentialsProviderByName(pull.getRemote())).call().toString());
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		} 
	}

	public void gitPush(String remoteName) throws ExceptionDBGit {
		try {
			Iterable<PushResult> result = git.push()
					.setCredentialsProvider(getCredentialsProviderByName(remoteName.equals("") ? Constants.DEFAULT_REMOTE_NAME : remoteName))
					.setRemote(remoteName.equals("") ? Constants.DEFAULT_REMOTE_NAME : remoteName).call();
			
			result.forEach(pushResult -> {
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
					throw new ExceptionDBGit("Dir not found!");
				}
				init.setDirectory(dir);
			}
			
			init.call();
			
			ConsoleWriter.println("Repository created");
			
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		} 		
	}
	
	public static void gitClone(String link, String remoteName) throws ExceptionDBGit {
		try {
			Git.cloneRepository().setURI(link).setCredentialsProvider(getCredentialsProvider(link))
				.setRemote(remoteName.equals("") ? Constants.DEFAULT_REMOTE_NAME : remoteName).call();
			
			ConsoleWriter.println("Repository cloned");
			
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
					
					ConsoleWriter.printlnGreen("Remote added");
					
					break;
				}
								
				case "remove" : {
					RemoteRemoveCommand remote = git.remoteRemove();
					remote.setName(name);
					remote.call();
					
					ConsoleWriter.printlnGreen("Remote removed");
					
					break;
				}
				
				default : ConsoleWriter.println("Unknown command");
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
			ConsoleWriter.println("Done");
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}
	
	private CredentialsProvider getCredentialsProviderByName(String remoteName) throws ExceptionDBGit {
		
		String link = git.getRepository().getConfig().getString("remote", remoteName, "url");
		
		if (link != null)
			return getCredentialsProvider(link);
		else
			throw new ExceptionDBGit("Cannot find " + remoteName);
	}
	
	private CredentialsProvider getCredentialsProvider() throws ExceptionDBGit {
		
		return getCredentialsProvider(git.getRepository().getConfig().getString("remote", Constants.DEFAULT_REMOTE_NAME, "url"));
	}
	
	private static CredentialsProvider getCredentialsProvider(String link) throws ExceptionDBGit {
		try {			
			Pattern patternPass = Pattern.compile("(?<=:(?!\\/))(.*?)(?=@)");
			Pattern patternLogin = Pattern.compile("(?<=\\/\\/)(.*?)(?=:(?!\\/))");
			
			String login = "";
			String pass = "";
			
			Matcher matcher = patternPass.matcher(link);
			if (matcher.find())
			{
				pass = matcher.group();				
			} else {
				throw new ExceptionDBGit("Can't find git password");
			}
			
			matcher = patternLogin.matcher(link);
			if (matcher.find())
			{
				login = matcher.group();				
			} else {
				throw new ExceptionDBGit("Can't find git login");
			}
	
			return new UsernamePasswordCredentialsProvider(login, pass);
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		} 
	}

}
