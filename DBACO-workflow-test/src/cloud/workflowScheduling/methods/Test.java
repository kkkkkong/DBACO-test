package cloud.workflowScheduling.methods;

import java.util.*;

import cloud.workflowScheduling.*;
import cloud.workflowScheduling.setting.*;

public class Test implements Scheduler{
    private double theta = 2;
    private List<Task> allocated;
    private List<Task> unallocated;
    private Map<Task, Double> actualFinishTime;
    private Map<Task, Double> minCost;
    private Map<Task, Double> leastNeedTime;
    private Map<Task, Double> maxNeedTime;
    public Test(double theta) {
    	this.theta = theta;
    }
    public double getTheta() {
    	return theta;
    }
    
    public Solution schedule(Workflow wf) {
    	allocated = new ArrayList<>();
    	unallocated = new ArrayList<>();
    	actualFinishTime = new HashMap<>();
    	minCost = new HashMap<>();
    	leastNeedTime = new HashMap<>();
    	maxNeedTime = new HashMap<>();
    	
    	
    		
    	wf.calcPURank(theta);
    	List<Task> tasks = new ArrayList<Task>(wf);
    	Collections.sort(tasks, new Task.PURankComparator());
    	Collections.reverse(tasks);
    	
    	return buildViaTaskList(wf, tasks, wf.getDeadline());
    }
    
    Solution buildViaTaskList(Workflow wf, List<Task> tasks, double deadline) {
    	Benchmarks backUp = new Benchmarks(wf);
    	double endTime1 = backUp.getFastSchedule().calcMakespan();
    	double endTime2 = backUp.getCheapSchedule().calcMakespan();
    	
    	for(Task task : tasks) {
    	  for(VM vm : backUp.getFastSchedule().keySet()) {
    		  for(Allocation alloc : backUp.getFastSchedule().get(vm)) {
    			  if(alloc.getTask()==task) {
    				  double end = alloc.getFinishTime();
    				  double left = endTime1 - end;
    				  leastNeedTime.put(task, left);
    			  }
    			  else continue;
    		  }
    	  }
    	  for(VM vm : backUp.getCheapSchedule().keySet()) {
    		  for(Allocation alloc : backUp.getCheapSchedule().get(vm)) {
    			  if(alloc.getTask()==task) {
    				  double end = alloc.getFinishTime();
    				  double left = endTime2 - end;
    				  maxNeedTime.put(task, left);
    			  }
    		  }
    	  }
    	}
    	//calculateCost(tasks);
    	return allocateTasks(wf, tasks, deadline);
    }
    
    private void calculateCost(List<Task> tasks) {
    	for(Task task : tasks) {
    		double min = task.getTaskSize() / VM.SPEEDS[VM.FASTEST];
    		System.out.println("Task:" + task + "'s minCost:" + min);
    		minCost.put(task, min);
    	}
    }
    
    Solution allocateTasks(Workflow wf, List<Task> tasks, double deadline) {
    	Solution solution = new Solution();
    	/*for(Task task : tasks) {
    		unallocated.add(task);
    	}*/
    	double CPLength = wf.get(0).getpURank();
    	for(int i=1; i<tasks.size();i++) {
    		
    		Task task = tasks.get(i);
    		
    		//double allocatedMax = 0;
    		double unallocatedSum = 0;
    		/*for(Task done : allocated) {
    			allocatedMax = Math.max(allocatedMax, actualFinishTime.get(done));
    		}*/
    		//allocatedMax = Math.max(allocatedMax, solution.calcMakespan());
    		
      		//System.out.println("allocated:" + allocatedMax);
    		/*for(Task steady : unallocated) {
    			unallocatedSum += leastNeedTime.get(steady);
    		}*/
      		unallocatedSum = (leastNeedTime.get(task)*0.999 + maxNeedTime.get(task)*0.001);
    		//System.out.println("unallocated:" + unallocatedSum);
    		//System.out.println("deadline:" + deadline);
    		double subDeadline = deadline  - unallocatedSum;
    		System.out.println("Task: "+ task+"'sSubDeadline:" + subDeadline);
    		//System.out.println("proSubDeadline: " + proSubDeadline);
    	    //subDeadline =(subDeadline + proSubDeadline)/2;
    	    
    		Allocation alloc = getMinCostVM(task, solution ,subDeadline, i);
    		System.out.println("Test:" + alloc);
    		if(alloc == null){
				alloc = getMinEFTVM(task, solution, subDeadline, i);
				
				VM vm = alloc.getVM();
				while(alloc.getFinishTime() > subDeadline + Evaluate.E && vm.getType() < VM.FASTEST){
					solution.updateVM(vm);	
					alloc.setStartTime(solution.calcEST(task, vm));
					alloc.setFinishTime(solution.calcEST(task, vm) + task.getTaskSize()/vm.getSpeed());
				}
			}
    		if(i == 1)		//after allocating task_1, allocate entryTask to the same VM 
				solution.addTaskToVM(alloc.getVM(), tasks.get(0), alloc.getStartTime(), true);
			solution.addTaskToVM(alloc.getVM(), task, alloc.getStartTime(), true);
			
			
			
			//actualFinishTime.put(task, alloc.getStartTime() + task.getTaskSize()/alloc.getVM().getSpeed() + maxOutTime);
			//allocated.add(task);
			//unallocated.remove(task);
    	}
    	return solution;
    }
    
    private Allocation getMinCostVM(Task task, Solution solution, double subDeadline, int taskIndex){
		double minIncreasedCost = Double.MAX_VALUE;	//increased cost for one VM is used here, instead of total cost
		VM selectedVM = null;
		double selectedStartTime = 0;
		
		double maxOutTime = 0;	//maxTransferOutTime
		for(Edge e : task.getOutEdges())
			maxOutTime = Math.max(maxOutTime, e.getDataSize());
		maxOutTime /= VM.NETWORK_SPEED;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that meets sub-deadline and minimizes the cost
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

		//test whether a new VM can meet the sub-deadline and (or) reduce increasedCost; if so, add this new VM
		int selectedI = -1;				
		startTime = taskIndex==1 ? VM.LAUNCH_TIME : solution.calcEST(task, null);
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
    
    private Allocation getMinEFTVM(Task task, Solution solution, double subDeadline, int taskIndex){
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
			startTime = taskIndex==1 ? VM.LAUNCH_TIME : solution.calcEST(task, null);
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
