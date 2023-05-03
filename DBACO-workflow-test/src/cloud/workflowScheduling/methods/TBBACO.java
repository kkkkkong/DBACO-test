package cloud.workflowScheduling.methods;

import java.io.IOException;
import java.util.*;

import javax.swing.plaf.SliderUI;

import cloud.workflowScheduling.Evaluate;
import cloud.workflowScheduling.setting.Allocation;
import cloud.workflowScheduling.setting.Edge;
import cloud.workflowScheduling.setting.Solution;
import cloud.workflowScheduling.setting.Task;
import cloud.workflowScheduling.setting.VM;
import cloud.workflowScheduling.setting.Workflow;


public class TBBACO implements Scheduler {
	private final double bestVMSpeed = VM.SPEEDS[VM.FASTEST];
	double userDeadline;
	private double theta = 2;
	private Map<Double,List<Task>> BOT;
	private Map<Task,Double> level;
	
	private Map<Double,Double> plevel_subD;
	private Map<Double,Double> level_subD;
	
	private TACO taco = new TACO();
	
	
	public TBBACO(double theta){
		this.theta = theta;
	}
	public double getTheta() {
		return theta;
	}
	
	public Solution schedule(Workflow wf) {
		
		wf.calcPURank(theta);
		List<Task> wtasks = new ArrayList<Task>(wf);
		
		userDeadline = wf.getDeadline();
		
		Task entry = wf.get(0);
		Task exit = wf.get(wf.size()-1);
		BOT = new HashMap<>();
		level = new HashMap<>();
		plevel_subD = new HashMap<>();
		level_subD = new HashMap<>();
		
		calculateTasksEST(wtasks);
		
		calculateLevels(wtasks);
		calculateBagOfTasks(wtasks);
		printBoT();
		calculateAllLevelSubD();
		
		return allocateBoTasks(entry,exit);
	}
	
	
    private void calculateTasksEST(List<Task> wtasks) {
		Task entryTask = wtasks.get(0);
		entryTask.setAST(0);
		entryTask.setAFT(0);
		entryTask.setAssigned(true);
		
		for(int i=1; i<wtasks.size(); i++){		// compute EST, EFT, critical parent via Eqs. 1 and 2; skip entry task
			//��1��ʼ��Ϊ��������ڽڵ�
			Task task = wtasks.get(i);

			//�˴�EST���岻����resource��available time���һ�Ҫ����critical parent������û��ʹ��solution.calcEST����
			//��������EST��EFT���ڵ���֮ǰ��
			double EST = -1;
			for(Edge e: task.getInEdges()){
				Task parent = e.getSource();
				double startTime = e.getDataSize()/VM.NETWORK_SPEED;
				//if assigned, use AFT; otherwise, use EFT
				startTime += parent.getEFT();
				EST = Math.max(EST, startTime);				//determine EST
			}
				task.setEST(EST);
				task.setEFT(EST + task.getTaskSize() / bestVMSpeed);
		}
		Task exitTask = wtasks.get(wtasks.size()-1);	//Note, EST, EFT, critialParent of exitTask have been set above
		exitTask.setAFT(userDeadline);
		exitTask.setAST(userDeadline);
	}
	
	
	private void printBoT() {
		int len = BOT.size(); 
		for(int i = 1; i < len; i++) {
			List<Task> ltasks = BOT.get((double)i);
			
			 Collections.sort(ltasks,new Comparator<Task>() {

					@Override
					public int compare(Task o1, Task o2) {
						// TODO Auto-generated method stub
						if(o1.getEST() > o2.getEST())
							return -1;
						if(o1.getEST() < o2.getEST())
							return 1;
						if(o1.getEST() == o2.getEST()) {
							if(o1.getTaskSize() > o2.getTaskSize())
								return 1;
							if(o1.getTaskSize() < o2.getTaskSize())
								return -1;
							return 0;
						}
						return 0;
					}
		    		   
		    	   });
			
			//System.out.println("�� " + i + " �� ���� " + ltasks.size() + "������");
			
			for(Task task : ltasks) {
	    		//System.out.println("task : " + task.getId() + " #" + "EST is # "+task.getEST()+" #");
	    	}
		}
	}
	
	private void calculateLevels(List<Task> tasks) {
        for (Task task : tasks) {
            calculateLevel(task);
        }
    }
    
    private void calculateLevel(Task task) {
    	
    	if(!level.containsKey(task)) {
    		List<Edge> inE = task.getInEdges();
    		if(inE.size() == 0) {
    			level.put(task, 0.0);
    			task.settLevel(0.0);
    			return;
    		}
    		double max_level = 0;
    		for(Edge e : task.getInEdges()) {
    			Task parent = e.getSource();
    			if(!level.containsKey(parent)) {
	            	calculateLevel(parent);
	            }
    		}
    		for(Edge e : task.getInEdges()) {
    			Task parent = e.getSource();
    			max_level = Math.max(max_level,level.get(parent));
    		}
    		level.put(task, max_level+1.0);	
    		task.settLevel(max_level+1.0);
    		}
  }
    
    //ͬһlevel������
    private void calculateBagOfTasks(List<Task> tasks) {
    	for (Task task : tasks) {
    		 if(!BOT.containsKey(level.get(task))) {
	            	List<Task> ltasks = new ArrayList<Task>();
	            	ltasks.add(task);
	            	BOT.put(level.get(task), ltasks);
	            }else {
	            	List<Task> ltasks = BOT.get(level.get(task));
	            	ltasks.add(task);
	            	BOT.put(level.get(task), ltasks);
	            }
        }
    	//System.out.println("# ���ϵ��»���Ϊ �� " + BOT.keySet().size() + " �㣡");
    }
    
    
    private void calculateAllLevelSubD() {
    	double dfs = 0.0;
        for(int i = 1; i < BOT.size() - 1; i++) {
     	   List<Task> tasks = BOT.get((double)i);
     	  calculatePreLevelSubD((double)i,tasks);
     	  if(i == 1) {
     		  dfs += tasks.size()* plevel_subD.get((double)i);
     	  }else {
     		  dfs += tasks.size()* (plevel_subD.get((double)i) - plevel_subD.get((double)(i-1)));
     	  }
        }
        
        double spareTime = userDeadline - plevel_subD.get((double)plevel_subD.keySet().size() - 1);
        System.out.println("����ʱ���ǣ�" + spareTime);
        if(spareTime >= 0) {
	        double DF = spareTime / dfs;
	        for(int i = 1; i < BOT.size() - 1; i++) {
	            double usedTime = 0.0;
	        	List<Task> tasks = BOT.get((double)i);      		   
	        		if(i == 1) {
	        			usedTime += DF*tasks.size()*plevel_subD.get((double)i);
	           		   	level_subD.put((double)i, usedTime+plevel_subD.get((double)i));
	        		}else {
	        			usedTime += DF*tasks.size()*(plevel_subD.get((double)i) - plevel_subD.get((double)(i-1)));
	        			level_subD.put((double)i, usedTime+level_subD.get((double)(i-1))+(plevel_subD.get((double)i) - plevel_subD.get((double)(i-1))));
	        		}
	      		   System.out.println("### LEVEL #### " + i + " ##### subD is : #### " + level_subD.get((double)i)+" ####");
	      	  }
        }else {
        	for(int i = 1; i < BOT.size() - 1; i++) {   
        			double reduce = 0.0;
	        		if(i == 1) {
	        			reduce = spareTime * ( plevel_subD.get((double)i) / plevel_subD.get((double)(BOT.size() - 2)) );
	           		   	level_subD.put((double)i, reduce+plevel_subD.get((double)i));
	        		}else {
	        			reduce = spareTime * ((plevel_subD.get((double)i) - plevel_subD.get((double)(i-1))) /  plevel_subD.get((double)(BOT.size() - 2)) );
	        			level_subD.put((double)i, reduce+plevel_subD.get((double)(i)));
	        		}
	      		   System.out.println("### LEVEL #### " + i + " ##### subD is : #### " + level_subD.get((double)i)+" ####");
	      	  }
        	
        }
        level_subD.put((double)BOT.size() - 1, userDeadline);
     }
    
	
    private double calculatePreLevelSubD(Double key,List<Task> tasks) {
    	double subD = Double.MIN_VALUE;
    	if(key == 1.0) {
      	   for(Task task : tasks) {
      		   if(task.minExeTime() > subD) {
      			 subD = task.minExeTime();
      		   }
      	   }
      	   plevel_subD.put(key, subD);
      	   //System.out.println("### LEVEL #### " + key + " ##### psubD is : #### " + plevel_subD.get(key)+" ####");
      	   return plevel_subD.get(key);
    	}else {
    		for(Task task : tasks) {
    			double taskmine = task.minExeTime();
    			subD = Math.max(subD,taskmine);
       	   }
    		//System.out.println("### LEVEL #### " + key + " ##### max is : #### " + subD+" ####");
    		plevel_subD.put(key, subD + plevel_subD.get(key - 1.0));
    		//System.out.println("### LEVEL #### " + key + " ##### psubD is : #### " + plevel_subD.get(key)+" ####");
    		return plevel_subD.get(key);
    	}
     }
    
    
    
    private Solution allocateBoTasks(Task entry,Task exit){
    	
    	Solution solution = new Solution();
    	
    	int len = BOT.size();     	
//    	System.out.println("�������" + solution);
    	//入口任务的调度为
    	Allocation alloc = getEntryVM(entry,solution);
    	solution.addTaskToVM(alloc.getVM(), entry, alloc.getStartTime(), true);
    
//    	System.out.println("��ڽ������" + solution);
    	
		for(int i = 1; i <len-1; i++) {//len - 1
			List<Task> ltasks = BOT.get((double)i);
			//System.out.println("��" + i + "�㹲��" + ltasks.size() + "������");
			solution = allocateBotTask(ltasks, level_subD.get((double)i),solution);
		
		}
//		System.out.println("û����ӳ�������");
//		System.out.println(solution);
//		System.out.println("Ѱ�ҳ�������!!!!");
		
		//出口任务的调度
		alloc = getExitVM(exit, solution);
		solution.addTaskToVM(alloc.getVM(), exit, alloc.getStartTime(), true);
		double totalCost = solution.calcCost();
		System.out.println("###总花费：" + totalCost);
		return solution;
    }
    
    Solution allocateBotTask(List<Task> tasks, double SubDeadline,Solution solution) {
		return taco.schedule(tasks, SubDeadline,solution);
	}
    
	//为入口任务分配开始时间和虚拟机
	public Allocation getEntryVM(Task entryTask,Solution sol) {
		//计算工作流的最早开始时间
		double startTime = sol.calcEST(entryTask, null);
		VM selectedVM = new VM(VM.SLOWEST);//选取最慢的虚拟机
		double selectedStartTime = startTime;
		return  new Allocation(selectedVM, entryTask, selectedStartTime);
	}
	
	public Allocation getExitVM(Task exitTask,Solution sol) {
		VM selectedVM = null;				
		double selectedStartTime = 0;
		double lft = Double.MIN_VALUE;
		//遍历sol中所有调用的虚拟机的准备完毕时间
		//出口任务的时间和虚拟机设置为有最晚准备完毕时间的虚拟机
		for(VM vm : sol.keySet()) {
			if(lft < sol.getVMReadyTime(vm)) {
				lft = sol.getVMReadyTime(vm);
				selectedVM = vm;
				selectedStartTime = lft;
			};
		}

		return  new Allocation(selectedVM, exitTask, selectedStartTime);
	}
}