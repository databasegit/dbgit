package ru.fusionsoft.dbgit;


import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.DepthWalk.RevWalk;
import org.eclipse.jgit.revwalk.RevCommit;

import ru.fusionsoft.dbgit.core.DBGit;

//This class for experiment
public class Experiment {

	public static void main(String[] args) throws Exception {
		Experiment ex = new Experiment();
		ex.test();		
		
		//main2(args);
	}
	
	
    public void test() throws Exception {
    	DBGit git = DBGit.getInctance();
    	
    	System.out.println(git.getRepository().getDirectory().getAbsolutePath());
    	
    	System.out.println(git.getRepository().getBranch());
    	
    	System.out.println(git.getRootDirectory());
    	
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

