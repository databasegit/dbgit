package ru.fusionsoft.dbgit.command;

import javax.xml.validation.meta.IMetaObject;
import javax.xml.validation.meta.MetaObjectFactory;

public class CmdRm implements IDBGitCommand {

	public void execute(String[] args)  throws Exception {
		// TODO Auto-generated method stub
		String name = "name obj from args";
		
		IMetaObject obj = MetaObjectFactory.createMetaObject(name);

		
		//obj.deSerialize(stream);
		
		//find obj by name and delete file 
		//удалить из гит этот файл
	}


}
