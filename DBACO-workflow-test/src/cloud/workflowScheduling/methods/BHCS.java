package cloud.workflowScheduling.methods;

import java.util.*;

import cloud.workflowScheduling.*;
import cloud.workflowScheduling.setting.*;

public class BHCS implements Scheduler{
    double budget;
    double RB;
    double RCB;
    private Map<Task, Map<Integer, Double>> standardCosts;
    private Map<Task, Double> actualCost;
    private Map<Task, Double> minCosts;
    private Map<VM, Double> unitPrices;
    private Map<Task,Map<Integer, Double>> EFT;
    private Map<Task,Map<VM, Double>> worth;
    private Map<Task, Double> bestFT;
    private Map<Task, Double> worstFT;
    private Map<Task, Double> highestCost;
    private Map<Task, Double> lowestCost;
    private Map<Task, Double> bestCost;
    private Map<Task, VM> taskTBestVM;
    private Map<Task, VM> taskTWorstVM;
	
    public BHCS() {
    	
    }
    
    public Solution schedule(Workflow wf) {
    	Benchmarks benSched = new Benchmarks(wf);
    	budget = wf.getBudget();
    	if(benSched.getFastSchedule().calcCost() < budget) {
    		return benSched.getFastSchedule();
    	}
    	wf.calcRank();
    	List<Task> wfTasks = new ArrayList<Task>(wf);
    	Collections.sort(wfTasks, new Task.rankComparator());
    	Collections.reverse(wfTasks);
    	
    	RB = wf.getBudget();
    	RCB = benSched.getCheapSchedule().calcCost();
    	
    	standardCosts = new HashMap<>();
    	actualCost = new HashMap<>();
    	minCosts = new HashMap<>();
    	unitPrices = new HashMap<>();
    	EFT = new HashMap<>();
    	worth = new HashMap<>();
    	bestFT = new HashMap<>();
    	worstFT = new HashMap<>();
    	highestCost = new HashMap<>();
    	lowestCost = new HashMap<>();
    	bestCost = new HashMap<>();
    	taskTBestVM = new HashMap<>();
    	taskTWorstVM = new HashMap<>();
    	
    	return buildViaTaskList(wf, wfTasks, wf.getBudget());
    }
    
    Solution buildViaTaskList(Workflow wf, List<Task> wfTasks, double budget) {
    	Solution solution = new Solution();
    	for(int i=0;i<VM.TYPE_NO;i++) {
    		VM initial = new VM(i);
    		solution.put(initial, new LinkedList<Allocation>());
    	}
    	budget = wf.getBudget();
    	calculateCost(wfTasks, solution);
    	calculateTasksEST(wfTasks);
    	
    	
    	return allocateTasks(wf, solution, wfTasks);
    }
    
    private void calculateCost(List<Task> tasks ,Solution solution) {
    	for(Task task : tasks) {
    		double maxSpeed = VM.SPEEDS[VM.FASTEST];
    		double minCost = Double.MAX_VALUE;
    		double maxCost = 0.0;
    		Map<Integer, Double> typeToCost = new HashMap<>();
    		for(VM vm : solution.keySet()) {
    			//typeToCost.put(i, 0.0);
    			double ratio = VM.SPEEDS[vm.getType()]/maxSpeed;
    			double unitPrice = ratio * (1 + ratio) /2;
    			unitPrices.put(vm, unitPrice);
    			double cost = (task.getTaskSize()/ VM.SPEEDS[vm.getType()])*unitPrice ;
    			//System.out.println("%%%%%%%%%"+cost);
    			minCost = Math.min(minCost, cost);
    			maxCost = Math.max(maxCost, cost);
    		}
    		lowestCost.put(task, minCost);
    		highestCost.put(task, maxCost);
    		//standardCosts.put(task, typeToCost);

    		for(VM vm : solution.keySet()) {   		
    			double cost = (task.getTaskSize()/VM.SPEEDS[vm.getType()]) * unitPrices.get(vm);
    			//System.out.println(task + "### cost ###:" + cost);
    			typeToCost.put(vm.getType(), cost);
    			standardCosts.put(task, typeToCost);
    		}
    	}
    }
    
    Solution allocateTasks(Workflow wf, Solution solution, List<Task> wfTasks) {
    	//VM lastVM = null;
		//Allocation lastAlloc = null;
		for(int i=1; i<wfTasks.size(); i++) {
			Task task = wfTasks.get(i);
			//System.out.println(task + "@@lowest@@:" + lowestCost.get(task));
			RCB = RCB - lowestCost.get(task);
			double costCoeff = RCB / RB;
			Allocation alloc = null;
			//if(i == wfTasks.size()-1) {
			//	alloc = new Allocation(lastVM, task, solution.calcMakespan());
			//}
			if(i == wfTasks.size()-1) {
				alloc = getMinCostVM(task, solution, Double.MAX_VALUE, i);
			}
			else  alloc = allocateTask(task, solution, costCoeff, i);

			actualCost.put(task, (task.getTaskSize()/alloc.getVM().getSpeed()) * unitPrices.get(alloc.getVM()));
			
			if(i == 1)		//after allocating task_1, allocate entryTask to the same VM 
				solution.addTaskToVM(alloc.getVM(), wfTasks.get(0), alloc.getStartTime(), true);
			
			//if(i == wfTasks.size()-2) {
			//	lastVM = alloc.getVM();
			//	lastAlloc = alloc;
			//}
			solution.addTaskToVM(alloc.getVM(), task, alloc.getStartTime(), true);
		}		
		return solution;
	}
    
    private void calculateTasksEST(List<Task> wtasks) {
		Task entryTask = wtasks.get(0);
		entryTask.setAST(0);
		entryTask.setAFT(0);
		entryTask.setAssigned(true);
		
		for(int i=1; i<wtasks.size(); i++){		// compute EST, EFT, critical parent via Eqs. 1 and 2; skip entry task
            Map<Integer, Double> typeToTime = new HashMap<>();
			Task task = wtasks.get(i);
			double EST = -1;
			for(Edge e: task.getInEdges()){
				Task parent = e.getSource();
				double startTime = e.getDataSize()/VM.NETWORK_SPEED;
				//if assigned, use AFT; otherwise, use EFT
				startTime += parent.getEFT();
				EST = Math.max(EST, startTime);				//determine EST
			}
				task.setEST(EST);
				task.setEFT(EST + task.getTaskSize() / VM.SPEEDS[VM.FASTEST]);
				for(int j =0;j<VM.TYPE_NO;j++) {
				    double FT = EST + task.getTaskSize() / VM.SPEEDS[j];
				    typeToTime.put(j, FT);
				}
				EFT.put(task, typeToTime);
		}
	}
    
    
    private Allocation allocateTask(Task task, Solution solution, double costCoeff, int taskIndex) {
    	getBestVM(task, solution, taskIndex);
		getWorstVM(task, solution, taskIndex);
    	double worthiness = 0.0;
    	double maxValue = Double.NEGATIVE_INFINITY;
    	double selectedStartTime = 0.0;
    	VM best = null;
    	for(VM vm : solution.keySet()) {
			worthiness = calculateWorth(task, vm, costCoeff);
			if(worthiness > maxValue) {
				maxValue = worthiness;
				best = vm;
				if(taskIndex == 1) {
					selectedStartTime  = VM.LAUNCH_TIME;
				}
				else selectedStartTime = solution.calcEST(task, best);
			}
		}
    	return new Allocation(best, task, selectedStartTime);
    }
    
    private void getBestVM(Task task, Solution solution, int taskIndex) {
    	VM best = null;
    	double bestF = Double.MAX_VALUE;
    	double startTime=0, finishTime;
    	
    	for(VM vm : solution.keySet()) {
    		if(taskIndex == 1) {
    			startTime = VM.LAUNCH_TIME;
    		}
    		else startTime = solution.calcEST(task, vm);
    		finishTime = startTime + task.getTaskSize() / vm.getSpeed();
    		if(finishTime < bestF) {
    			bestF = finishTime;
    			best = vm;
    		}
    		System.out.println("bestF:" + bestF);
    	}
        bestFT.put(task, bestF);
        bestCost.put(task, (task.getTaskSize()/best.getSpeed())*unitPrices.get(best));
    	taskTBestVM.put(task, best);
    }
    
    private void getWorstVM(Task task, Solution solution, int taskIndex) {
    	VM worst = null;
    	double worstF = 0.0;
    	double startTime, finishTime;
    	
    	for(VM vm : solution.keySet()) {
    		if(taskIndex == 1) {
    			startTime = VM.LAUNCH_TIME;
    		}
    		else startTime = solution.calcEST(task, vm);
    		finishTime = startTime + task.getTaskSize() / vm.getSpeed();
    		if(finishTime > worstF) {
    			worstF = finishTime;
    			worst = vm;
    		}
    	}
    	worstFT.put(task, worstF);
    	taskTWorstVM.put(task, worst);
    }
    
    private double calculateWorth(Task task, VM vm, double costCoeff) {
    	Map<VM, Double> worthy = new HashMap<>();
    	double worthiness = 0.0;
    	//System.out.println("costCoeff:" + costCoeff);
    	//System.out.println("worstFT:" + worstFT.get(task));
    	//System.out.println("EFT:" + EFT.get(task).get(vm.getType()));
    	//System.out.println("bestFT:" + bestFT.get(task));
    	//System.out.println("bestCost:" + bestCost.get(task));
    	//System.out.println("highestCost:" + highestCost.get(task));
    	//System.out.println("lowestCost:" + lowestCost.get(task));
    	double timeRatio = (worstFT.get(task) - EFT.get(task).get(vm.getType()))/(worstFT.get(task) - bestFT.get(task));
    	if(EFT.get(task).get(vm.getType()) == bestFT.get(task) || worstFT.get(task) == bestFT.get(task))
    		timeRatio = Double.POSITIVE_INFINITY;
    	System.out.println("timeRatio:" + timeRatio);
    	double costRatio = (bestCost.get(task) - standardCosts.get(task).get(vm.getType()))/(highestCost.get(task) - lowestCost.get(task));
    	if(standardCosts.get(task).get(vm.getType()) == lowestCost.get(task) || highestCost.get(task) == lowestCost.get(task))
    		costRatio = Double.POSITIVE_INFINITY;
    	System.out.println("costRatio:" + costRatio);
    	
    	
    	if(standardCosts.get(task).get(vm.getType()) > bestCost.get(task))
    		worthiness = Double.NEGATIVE_INFINITY;
    	else if(standardCosts.get(task).get(vm.getType()) > (RB - RCB))
    		worthiness = Double.NEGATIVE_INFINITY;
    	else worthiness = (costRatio * costCoeff + timeRatio);
    	System.out.println("worthiness:" + worthiness);
    	worthy.put(vm, worthiness);
    	worth.put(task, worthy);
    	return worthiness;
    }
    
    private Allocation getAssigned(Task task, Solution solution, VM bestVM, int taskIndex) {
    	double selectedStartTime = 0;
    	double startTime, finishTime;
    	
    	startTime = solution.calcEST(task, bestVM);
    	finishTime = startTime + task.getTaskSize()/bestVM.getSpeed();
    	
    	return new Allocation(bestVM, task, selectedStartTime);
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
    
}
