package cloud.workflowScheduling.methods;

import java.util.*;

import cloud.workflowScheduling.*;
import cloud.workflowScheduling.setting.*;

public class TestNew implements Scheduler{
    double budget;
    double RB;
    double RCB;
    private HashMap<Task, Map<VM, Double>> costOnVM;
    private HashMap<Task, Double> maxCostOnVM;
    private HashMap<Task, Double> minCostOnVM;
    private HashMap<Task, Double> actualCost;
    
	private double theta = 2;
    
	public TestNew(double theta) {
		this.theta = theta;
	}
	public double getTheta() {
		return theta;
	}
	public Solution schedule(Workflow wf) {
		actualCost = new HashMap<>();
		costOnVM = new HashMap<>();
		maxCostOnVM = new HashMap<>();
		minCostOnVM = new HashMap<>();
		wf.calcPURank(theta);
		List<Task> wfTasks = new ArrayList<Task>(wf);
		Collections.sort(wfTasks, new Task.PURankComparator());
		Collections.reverse(wfTasks);
		
		
		return buildViaTaskList(wf, wfTasks, wf.getDeadline());
	}
	
	Solution buildViaTaskList(Workflow wf, List<Task> tasks, double deadline) {
		deadline = wf.getDeadline();
		Solution solution = new Solution();
		double CPLength = wf.get(0).getpURank();
		
		for(int i = 1; i<tasks.size();i++) {
			Task task = tasks.get(i);
			double proSubDeadline = (CPLength - task.getpURank() + task.getTaskSize()/VM.SPEEDS[VM.FASTEST])
					/CPLength *deadline;
			//Allocation alloc = getWorthyVM(task, solution, proSubDeadline, i);
			Allocation alloc = getMinCostVM(task, solution, proSubDeadline, i);
			if(alloc == null) {
                alloc = getMinEFTVM(task, solution, proSubDeadline, i);
                
				VM vm = alloc.getVM();
				while(alloc.getFinishTime() > proSubDeadline + Evaluate.E && vm.getType() < VM.FASTEST){
					solution.updateVM(vm);			
					alloc.setStartTime(solution.calcEST(task, vm));
					alloc.setFinishTime(solution.calcEST(task, vm) + task.getTaskSize()/vm.getSpeed());
				}
			}
			
			double maxOutTime = 0;
			for(Edge e : task.getOutEdges())//OutEdges:从task出发连向其他任务的边
				maxOutTime = Math.max(maxOutTime, e.getDataSize());//遍历取得最大的出边上的数据大小
			maxOutTime /= VM.NETWORK_SPEED;
			actualCost.put(task, ((task.getTaskSize()/alloc.getVM().getSpeed()+maxOutTime)/VM.INTERVAL) * alloc.getVM().getUnitCost());
			if(i == 1)		//after allocating task_1, allocate entryTask to the same VM 
				solution.addTaskToVM(alloc.getVM(), tasks.get(0), alloc.getStartTime(), true);
			solution.addTaskToVM(alloc.getVM(), task, alloc.getStartTime(), true);
		}
		Solution nsolution = greedyImprove(wf, tasks, solution, deadline);
		return nsolution;
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
			startTime = taskIndex == 1 ? VM.LAUNCH_TIME:solution.calcEST(task, vm); 
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			if(finishTime > subDeadline + Evaluate.E)   //sub-deadline not met
				continue;
			//首先要满足subDeadline约束，再在这些满足时间的里选择最小的
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
	private Allocation getWorthyVM(Task task, Solution solution, double subDeadline, int taskIndex) {
		Map<VM, Double> vmToCost = new HashMap<>();
		System.out.println("###Test:" + vmToCost);
		VM selectedVM = null;
		double maxCost = 0;
		double minCost = Double.MAX_VALUE;
		double bestRatio = 0;
		double selectedStartTime = 0;
		double startTime, finishTime;
		
		double maxOutTime = 0;	//maxTransferOutTime
		for(Edge e : task.getOutEdges())
			maxOutTime = Math.max(maxOutTime, e.getDataSize());
		maxOutTime /= VM.NETWORK_SPEED;
		
		
		for(VM vm : solution.keySet()) {
			startTime = solution.calcEST(task, vm); 
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			if(finishTime > subDeadline + Evaluate.E)   //sub-deadline not met
				continue;
			//首先要满足subDeadline约束，再在这些满足时间的里选择最小的
			double newVMPeriod = finishTime + maxOutTime - solution.getVMLeaseStartTime(vm);
			double newVMTotalCost = Math.ceil(newVMPeriod/VM.INTERVAL) * vm.getUnitCost();
			double cost = newVMTotalCost - solution.calcVMCost(vm);
			vmToCost.put(vm, cost);
			
			if(cost > maxCost)
				maxCost = cost;
			if(cost < minCost)
				minCost = cost;
		}
		System.out.println("###mid test:" + vmToCost);
		costOnVM.put(task, vmToCost);
		maxCostOnVM.put(task, maxCost);
		minCostOnVM.put(task, minCost);
		System.out.println("###cost check:" + costOnVM);
		for(VM vm : costOnVM.get(task).keySet()) {
			double cost = costOnVM.get(task).get(vm);
			double max = maxCostOnVM.get(task);
			double min = minCostOnVM.get(task);
			
			double ratio = (max - cost)/(max - min);
			if(ratio > bestRatio) {
				bestRatio = ratio;
				selectedVM = vm;
			}
		}
		selectedStartTime = taskIndex==1 ? VM.LAUNCH_TIME : solution.calcEST(task, selectedVM);
		
		vmToCost.clear();
		System.out.println("###Second test:" + vmToCost);
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
	
	Solution greedyImprove(Workflow wf, List<Task> tasks, Solution solution, double deadline) {
		Solution better = new Solution();
		double CPLength = wf.get(0).getpURank();
		for(VM vm : solution.keySet()) {
			int  type = vm.getType();
			better.put(new VM(type), new LinkedList<Allocation>());
		}
		
		System.out.println("###:"+ better.keySet());
		for(int i = 1;i<tasks.size();i++) {
			Task task = tasks.get(i);
			double proSubDeadline = (CPLength - task.getpURank() + task.getTaskSize()/VM.SPEEDS[VM.FASTEST])
					/CPLength *deadline;
			
			Allocation alloc = getMinCostVM(task, better, proSubDeadline, i);
			
			System.out.println();
			if(alloc == null) {
                alloc = getMinEFTVM(task, better, proSubDeadline, i);
                
				VM vm = alloc.getVM();
				while(alloc.getFinishTime() > proSubDeadline + Evaluate.E && vm.getType() < VM.FASTEST){
					better.updateVM(vm);			
					alloc.setStartTime(better.calcEST(task, vm));
					alloc.setFinishTime(better.calcEST(task, vm) + task.getTaskSize()/vm.getSpeed());
				}
			}
			if(i == 1)		//after allocating task_1, allocate entryTask to the same VM 
				better.addTaskToVM(alloc.getVM(), tasks.get(0), alloc.getStartTime(), true);
			better.addTaskToVM(alloc.getVM(), task, alloc.getStartTime(), true);
		}
		return better;
	}
	
	Solution greedyImprovement(Workflow wf, List<Task> tasks, Solution solution, double deadline) {
		Solution better = new Solution();
		double CPLength = wf.get(0).getpURank();
		for(VM vm : solution.keySet()) {
			int  type = vm.getType();
			better.put(new VM(type), new LinkedList<Allocation>());
		}
		for(int i=tasks.size()-1;i>=1;i++) {
			Task task = tasks.get(i);
			double proSubDeadline = (CPLength - task.getpURank() + task.getTaskSize()/VM.SPEEDS[VM.FASTEST])
					/CPLength *deadline;
			Allocation alloc = getLessCostVM(task, solution, better, proSubDeadline, i);
		}
		return better;
	}
	
	private Allocation getLessCostVM(Task task, Solution old, Solution better, double proSubDeadline, int taskIndex) {
		VM betterVM = null;
		double betterStartTime = 0;
		double startTime=0, finishTime;
		double minCost = actualCost.get(task);
		double maxOutTime = 0;
		for(Edge e : task.getOutEdges())
			maxOutTime = Math.max(maxOutTime, e.getDataSize());
		maxOutTime /= VM.NETWORK_SPEED;
		
		for(VM vm : better.keySet()) {
			finishTime = proSubDeadline;
			startTime = finishTime - task.getTaskSize()/vm.getSpeed();
			if(startTime < 0)
				continue;
			
			double newVMPeriod = finishTime + maxOutTime - better.getVMLeaseEndTime(vm);
			double newVMTotalCost = Math.ceil(newVMPeriod/VM.INTERVAL) * vm.getUnitCost();
			double cost = newVMTotalCost - better.calcVMCost(vm);
			if(cost < minCost) {
				minCost = cost;
				betterVM = vm;
				betterStartTime = startTime;
			}
		}
		return new Allocation(betterVM, task, betterStartTime);
		
	}
}
