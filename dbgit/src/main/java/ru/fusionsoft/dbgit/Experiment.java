package ru.fusionsoft.dbgit;


import java.io.IOException;
import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.RawTextComparator;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.DepthWalk.RevWalk;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.eclipse.jgit.util.io.DisabledOutputStream;

import ru.fusionsoft.dbgit.core.DBGit;

//This class for experiment
public class Experiment {

	public static void main(String[] args) throws Exception {
		Experiment ex = new Experiment();
		ex.test();		
		
		//main2(args);
	}
	
	
    public void test() throws Exception {
    	DBGit dbGit = DBGit.getInctance();    	
    	System.out.println(dbGit.getRepository().getDirectory().getAbsolutePath());    	
    	System.out.println(dbGit.getRepository().getBranch());    	
    	System.out.println(dbGit.getRootDirectory());
    	
    	
    	List<String> files = dbGit.getGitIndexFiles("dbgit/src/main/java/ru/fusionsoft/dbgit/command/");
    	for (int i = 0; i < files.size(); i++) {
    		System.out.println(files.get(i));
    	}
    	
    	System.out.println("=======================================================");
    	
    	Repository repository =	dbGit.getRepository();
    	
    	Git git = new Git(repository);
    	
    	RevWalk walk = new RevWalk(repository, 10);
    	
    	
    	/*
    	ObjectId head = repository.resolve(Constants.HEAD);
        RevCommit commit = walk.parseCommit(head);
        */
    	
    	DirCache cache = repository.readDirCache();
    	//repository.di
    	
    	System.out.println("QQQQQ "+cache.findEntry("dbgit/src/main/java/ru/fusionsoft/dbgit/command/"));
    	
    	for (int i = 0; i < cache.getEntryCount(); i++) {
    		
    		System.out.println(cache.getEntry(i).getPathString());
    	}
    	
    	
    	System.out.println("!!!!!  " + repository.readDirCache().getEntryCount());
    			//getEntry("").getObjectId();
    	

    	
    	
    	System.out.println("=======================================================");
    	
    	Ref head = repository.findRef("HEAD");
    	RevCommit commit = walk.parseCommit(head.getObjectId());
    	
    	
        RevTree tree = commit.getTree();
        System.out.println("Having tree: " + tree);
        
    	TreeWalk treeWalk = new TreeWalk(repository);
    	treeWalk.addTree(tree);
    	treeWalk.setFilter(PathFilter.create("dbgit/src/main/java/ru/fusionsoft/dbgit/command/"));
    	//TreeFilter newFilter = new TreeFilter
    	//treeWalk.setFilter(newFilter);
    	
    	treeWalk.setRecursive(true);
    	while (treeWalk.next()) {
    	    if (treeWalk.isSubtree()) {
    	        System.out.println("dir: " + treeWalk.getPathString());
    	        treeWalk.enterSubtree();
    	    } else {
    	        System.out.println("file: " + treeWalk.getPathString());
    	    }
    	}
    	
    	/*
    	RevWalk rw = new RevWalk(repository, 10);
    	ObjectId head = repository.resolve(Constants.HEAD);
    	RevCommit commit = rw.parseCommit(head);
    	RevCommit parent = rw.parseCommit(commit.getParent(0).getId());
    	DiffFormatter df = new DiffFormatter(DisabledOutputStream.INSTANCE);
    	df.setRepository(repository);
    	df.setDiffComparator(RawTextComparator.DEFAULT);
    	df.setDetectRenames(true);
    	List<DiffEntry> diffs = df.scan(parent.getTree(), commit.getTree());
    	for (DiffEntry diff : diffs) {
    	    System.out.println(MessageFormat.format("({0} {1} {2}", diff.getChangeType().name(), diff.getNewMode().getBits(), diff.getNewPath()));
    	}
    	*/
    	
    }
    
    public static void main2(String[] args) throws IOException, GitAPIException {
        Repository repo = new FileRepository("../.git");

        
        System.out.println(repo.getDirectory().getAbsolutePath());
        System.out.println(repo.getBranch());
        
        
       
        
        Git git = new Git(repo);
        RevWalk walk = new RevWalk(repo, 1);

        List<Ref> branches = git.branchList().call();

        for (Ref branch : branches) {
            String branchName = branch.getName();

            System.out.println("Commits of branch: " + branch.getName());
            System.out.println("-------------------------------------");

            Iterable<RevCommit> commits = git.log().all().call();

            for (RevCommit commit : commits) {
                boolean foundInThisBranch = false;

                RevCommit targetCommit = walk.parseCommit(repo.resolve(
                        commit.getName()));
                for (Map.Entry<String, Ref> e : repo.getAllRefs().entrySet()) {
                    if (e.getKey().startsWith(Constants.R_HEADS)) {
                        if (walk.isMergedInto(targetCommit, walk.parseCommit(
                                e.getValue().getObjectId()))) {
                            String foundInBranch = e.getValue().getName();
                            if (branchName.equals(foundInBranch)) {
                                foundInThisBranch = true;
                                break;
                            }
                        }
                    }
                }

                if (foundInThisBranch) {
                    System.out.println(commit.getName());
                    System.out.println(commit.getAuthorIdent().getName());
                    System.out.println(new Date(commit.getCommitTime() * 1000L));
                    System.out.println(commit.getFullMessage());
                }
            }
        }
        
        walk.close();
        git.close();
    }
    
    

}

