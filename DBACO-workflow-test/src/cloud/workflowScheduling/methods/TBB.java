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


public class TBB implements Scheduler {
	private final double bestVMSpeed = VM.SPEEDS[VM.FASTEST];
	double userDeadline;
	private double theta = 2;
	private Map<Double,List<Task>> BOT;
	private Map<Task,Double> level;
	
	private Map<Double,Double> plevel_subD;
	private Map<Double,Double> level_subD;
	public Solution solution = new Solution();
	
	public TBB(double theta){
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
      	   System.out.println("### LEVEL #### " + key + " ##### psubD is : #### " + plevel_subD.get(key)+" ####");
      	   return plevel_subD.get(key);
    	}else {
    		for(Task task : tasks) {
    			double taskmine = task.minExeTime();
    			subD = Math.max(subD,taskmine);
       	   }
    		//System.out.println("### LEVEL #### " + key + " ##### max is : #### " + subD+" ####");
    		plevel_subD.put(key, subD + plevel_subD.get(key - 1.0));
    		System.out.println("### LEVEL #### " + key + " ##### psubD is : #### " + plevel_subD.get(key)+" ####");
    		return plevel_subD.get(key);
    	}
     }
    
    
    
    private Solution allocateBoTasks(Task entry,Task exit){
    	
    	int len = BOT.size();     	
    	System.out.println("�������" + solution);
    	
    	//solution.getRevMapping().clear();
    	Allocation alloc = getMinCostVM(entry, solution, userDeadline);
    	solution.addTaskToVM(alloc.getVM(), entry, alloc.getStartTime(), true);
    
    	System.out.println("��ڽ������" + solution);
    	
		for(int i = 1; i < len; i++) {
			List<Task> ltasks = BOT.get((double)i);
			allocateBotTask(ltasks, level_subD.get((double)i));
		
		}
		return solution;
    }
    
    Solution allocateBotTask(List<Task> tasks, double SubDeadline) {
		int violationCount = 0;		// test code
		for(int i = 0; i < tasks.size(); i++){		
			Task task = tasks.get(i);
			Allocation alloc = getMinCostVM(task, solution,SubDeadline);
			if(alloc == null){			//select a vm which allows EFT
				System.out.println(task.getId() + "δ�ҵ���С�����ѵ����");
				alloc = getMinEFTVM(task, solution, SubDeadline);
				
				VM vm = alloc.getVM();
				while(alloc.getFinishTime() > SubDeadline + Evaluate.E && vm.getType() < VM.FASTEST){
					solution.updateVM(vm);			//upgrade������������ĸ��£����ӶȽ�����̫�ࡣ
					alloc.setStartTime(solution.calcEST(task, vm));
					alloc.setFinishTime(solution.calcEST(task, vm) + task.getTaskSize()/vm.getSpeed());
				}
				if(alloc.getFinishTime() > SubDeadline + Evaluate.E)
					violationCount ++;
			}
			solution.addTaskToVM(alloc.getVM(), task, alloc.getStartTime(), true);
		}
		return solution;
	}
	private Allocation getMinCostVM(Task task, Solution solution, double subDeadline){
		double minIncreasedCost = Double.MAX_VALUE;	//increased cost for one VM is used here, instead of total cost
		VM selectedVM = null;
		double selectedStartTime = 0;
		
		double maxOutTime = 0;	//maxTransferOutTime
		for(Edge e : task.getOutEdges())
			maxOutTime = Math.max(maxOutTime, e.getDataSize());
		maxOutTime /= VM.NETWORK_SPEED;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that meets sub-deadline and minimizes the cost
		if(solution.size()!=0) {
			for(VM vm : solution.keySet()){	
				startTime = solution.calcEST(task, vm); 
				finishTime = startTime + task.getTaskSize()/vm.getSpeed();
				if(finishTime > subDeadline + Evaluate.E)   //sub-deadline not met
					continue;
				
				double newVMPeriod = finishTime + maxOutTime - solution.getVMLeaseStartTime(vm);
				double newVMTotalCost = Math.ceil(newVMPeriod/VM.INTERVAL) * vm.getUnitCost();
				double increasedCost = newVMTotalCost - solution.calcVMCost(vm);  // oldVMTotalCost
				if(increasedCost < minIncreasedCost){ 
					minIncreasedCost = increasedCost;
					selectedVM = vm;
					selectedStartTime = startTime;
				}
			}
		}

		//test whether a new VM can meet the sub-deadline and (or) reduce increasedCost; if so, add this new VM
		int selectedI = -1;				
		startTime =  solution.calcEST(task, null);
		for(int k = 0 ; k<VM.TYPE_NO; k++){
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[k];
			if(finishTime > subDeadline + Evaluate.E)	//sub-deadline not met
				continue;
			
			double increasedCost = Math.ceil((finishTime - startTime)/VM.INTERVAL) * VM.UNIT_COSTS[k];
			if(increasedCost < minIncreasedCost){
				minIncreasedCost = increasedCost;
				selectedI = k;
				selectedStartTime = startTime;
			}
		}
		if(selectedI != -1)
			selectedVM = new VM(selectedI);
		
		if(selectedVM == null)
			return null;
		else
			return new Allocation(selectedVM, task, selectedStartTime);
	}
	
	//select a VM from R which minimizes the finish time of the task
	//here, candidates only include services from R if R is not null
	private Allocation getMinEFTVM(Task task, Solution solution, double subDeadline){
		VM selectedVM = null;				
		double selectedStartTime = 0;
		double minEFT = Double.MAX_VALUE;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that minimizes EFT
		for(VM vm : solution.keySet()){			
			startTime = solution.calcEST(task, vm); 
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			if(finishTime < minEFT){
				minEFT = finishTime;
				selectedVM = vm;
				selectedStartTime = startTime;
			}
		}

		// if solution has no VMs 
		if(selectedVM==null ){		// logically, it is equal to "solution.keySet().size()==0"
			startTime = solution.calcEST(task, null);
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[VM.FASTEST];
			if(finishTime < minEFT){
				minEFT = finishTime;
				selectedStartTime = startTime;
				selectedVM = new VM(VM.FASTEST);
			}
		}
		return  new Allocation(selectedVM, task, selectedStartTime);
	}
}