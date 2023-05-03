package cloud.workflowScheduling.methods;

import java.util.*;

import cloud.workflowScheduling.*;
import cloud.workflowScheduling.setting.*;

public class BHEFT implements Scheduler {
	double deadline;
	double budget;
	private Map<Task, Double> averageCosts;
	private Map<Task, Map<Integer, Double>> standardCosts; 
	private Map<Task, Double> actualCost;
	private List<Task> allocated;
	private List<Task> unallocated;
	private List<VM> aType;
	
    public BHEFT() {
    	
    }
    
    public Solution schedule(Workflow wf) {
    	wf.calcRank();
    	List<Task> wfTasks = new ArrayList<Task>(wf);
    	Collections.sort(wfTasks, new Task.rankComparator());
    	Collections.reverse(wfTasks);
    	
    	averageCosts = new HashMap<>();
    	standardCosts = new HashMap<>();
		actualCost = new HashMap<>();
		allocated = new ArrayList<>();
		unallocated = new ArrayList<>();
		aType = new ArrayList<>();
		
		return buildViaTaskList(wf, wfTasks, wf.getDeadline());
    }
    
    Solution buildViaTaskList(Workflow wf, List<Task> wfTasks, double deadline) {
    	deadline = wf.getDeadline();
    	budget = wf.getBudget();
        //System.out.println("HEFT's Budget: " + budget);
    	
    	averageCosts = new HashMap<>();
		standardCosts = new HashMap<>();
		actualCost = new HashMap<>();
		allocated = new ArrayList<>();
		unallocated = new ArrayList<>();
		aType = new ArrayList<>();
		
		//System.out.println("##deadline:" + deadline);
		//System.out.println("##budget:" + budget);
		
		calculateCost(wfTasks);
		Solution solution = new Solution();
		solution = allocateTasks(wf, wfTasks, wf.getDeadline());
		return solution;
    }
    
    private void calculateCost(List<Task> tasks) {
    	for(Task task : tasks) {
    		double averageCost = 0;
    		double maxOutTime = 0;
    		Map<Integer, Double> typeToCost = new HashMap<>();
    		for(int i=0;i<VM.TYPE_NO;i++) {
    			typeToCost.put(i, 0.0);
    		}
    		standardCosts.put(task, typeToCost);
    		for(Edge e : task.getOutEdges())
    			maxOutTime = Math.max(maxOutTime, e.getDataSize());
    		maxOutTime /=VM.NETWORK_SPEED;
    		
    		for(int i=0;i<VM.TYPE_NO;i++) {   			
    			averageCost +=Math.ceil((task.getTaskSize()/VM.SPEEDS[i]+maxOutTime)/VM.INTERVAL) * VM.UNIT_COSTS[i];
    			typeToCost.put(i, Math.ceil((task.getTaskSize()/VM.SPEEDS[i]+maxOutTime)/VM.INTERVAL) * VM.UNIT_COSTS[i]);
    			standardCosts.put(task, typeToCost);
    		}
    		averageCost /=VM.TYPE_NO;
    		averageCosts.put(task, averageCost);
    	}
    }
    
    Solution allocateTasks(Workflow wf, List<Task> wfTasks, double deadline) {
        Solution solution = new Solution();
        
		for(Task task : wfTasks) {
			unallocated.add(task);
		}
		
		for(int i=1; i<wfTasks.size(); i++) {
			Task task = wfTasks.get(i);
			aType.clear();
			
			Allocation alloc = allocateTask(task, solution, deadline, i);
			System.out.println("Test here:" + alloc);
			double maxOutTime = 0;	//maxTransferOutTime
			for(Edge e : task.getOutEdges())//OutEdges:浠巘ask鍑哄彂杩炲悜鍏朵粬浠诲姟鐨勮竟
				maxOutTime = Math.max(maxOutTime, e.getDataSize());//閬嶅巻鍙栧緱鏈�澶х殑鍑鸿竟涓婄殑鏁版嵁澶у皬
			maxOutTime /= VM.NETWORK_SPEED;
			actualCost.put(task, ((task.getTaskSize()/alloc.getVM().getSpeed()+maxOutTime)/VM.INTERVAL) * alloc.getVM().getUnitCost());
			
			if(i == 1)		//after allocating task_1, allocate entryTask to the same VM 
				solution.addTaskToVM(alloc.getVM(), wfTasks.get(0), alloc.getStartTime(), true);
			
			solution.addTaskToVM(alloc.getVM(), task, alloc.getStartTime(), true);
			allocated.add(task);
			unallocated.remove(task);
		}
		return solution;
    }
    
    
    private Allocation allocateTask(Task task, Solution solution, Double proSubDeadline, int taskIndex) {
		Allocation alloc = null;
		double SAB=0.0;
		double CTB=0.0;
		double allocatedSum=0.0;
    	double unallocatedSum=0.0;
		
		for(Task proceed : allocated) {
			allocatedSum += actualCost.get(proceed); 
		}
		for(Task precedent : unallocated) {
			unallocatedSum +=averageCosts.get(precedent);
		}
		SAB = budget-allocatedSum-unallocatedSum;
		System.out.println("SAB::" + SAB);
		
		if(SAB>=0) {
    		CTB =averageCosts.get(task)+ Math.round(SAB*averageCosts.get(task)/unallocatedSum);
    	}else CTB = averageCosts.get(task);
		
		System.out.println("CTB::" + CTB);
		for(VM vm : solution.keySet()) {
			if(standardCosts.get(task).get(vm.getType())<CTB) {
				aType.add(vm);
			}
		}
		for(VM vm : aType) {
			System.out.println("VM in Atype::"+vm);
		}
		if(!aType.isEmpty()) {
			System.out.println("Go A");
			alloc = getProperVM(task, solution, proSubDeadline, CTB, taskIndex, aType);
			if(alloc == null)
				alloc = getMinEFTVM(task, solution, proSubDeadline, taskIndex);    
		}
		else if(SAB>=0) {
			System.out.println("Go B");
			alloc = getMinEFTVM(task, solution, proSubDeadline, taskIndex);
			
		}else {
			System.out.println("Go C");
			alloc = getMinCostVM(task, solution, proSubDeadline, taskIndex);
			if(alloc == null){//sl涓虹┖			//select a vm which allows EFT
				alloc = getMinEFTVM(task, solution, proSubDeadline, taskIndex);//閫夋嫨璇ユ儏鍐典笅鏈�蹇殑vm
			}
		}
		//VM vm = alloc.getVM();
		/*while(alloc.getFinishTime() > proSubDeadline + Evaluate.E && vm.getType() < VM.FASTEST){//褰撳墠鏈�蹇殑閮借秴杩囧瓙鎴鏃堕棿锛屼笖涓嶆槸鏈�蹇瓑绾х殑锛屽垯鍗囩骇
		    solution.updateVM(vm);			//upgrade锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷母锟斤拷拢锟斤拷锟斤拷佣冉锟斤拷锟斤拷锟教拷唷�
		    alloc.setStartTime(solution.calcEST(task, vm));
		    alloc.setFinishTime(solution.calcEST(task, vm) + task.getTaskSize()/vm.getSpeed());
	}*/
		return alloc;
	}
    
    private Allocation getMinEFTVM(Task task, Solution solution, double subDeadline, int taskIndex){
		VM selectedVM = null;				
		double selectedStartTime = 0;
		double minEFT = Double.MAX_VALUE;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that minimizes EFT
		for(VM vm : solution.keySet()){
			startTime = solution.calcEST(task, vm); //Problem
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			if(finishTime < minEFT){
				minEFT = finishTime;
				selectedVM = vm;
				selectedStartTime = startTime;
			}
		}
		// if solution has no VMs 
		if(selectedVM==null ){		// logically, it is equal to "solution.keySet().size()==0"
			System.out.println("ADDED ONE");
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
    
    private Allocation getMinCostVM(Task task, Solution solution, double subDeadline, int taskIndex){
		double minIncreasedCost = Double.MAX_VALUE;	//increased cost for one VM is used here, instead of total cost
		VM selectedVM = null;
		double selectedStartTime = 0;
		
		double maxOutTime = 0;	//maxTransferOutTime
		for(Edge e : task.getOutEdges())//OutEdges:浠巘ask鍑哄彂杩炲悜鍏朵粬浠诲姟鐨勮竟
			maxOutTime = Math.max(maxOutTime, e.getDataSize());//閬嶅巻鍙栧緱鏈�澶х殑鍑鸿竟涓婄殑鏁版嵁澶у皬
		maxOutTime /= VM.NETWORK_SPEED;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that meets sub-deadline and minimizes the cost
		for(VM vm : solution.keySet()){
			startTime = solution.calcEST(task, vm); 
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			//if(finishTime > subDeadline + Evaluate.E)   //sub-deadline not met
				//continue;
			//newVMPeriod 绉熺敤鏃堕暱
			double newVMPeriod = finishTime + maxOutTime - solution.getVMLeaseStartTime(vm);//finishTime + maxOutTime==LFT(Lease finish Time) 锛�9锛�
			double newVMTotalCost = Math.ceil(newVMPeriod/VM.INTERVAL) * vm.getUnitCost();//EC
			double increasedCost = newVMTotalCost; //-solution.calcVMCost(vm);   oldVMTotalCost //鍚庡崼鍘熸湰solution涓繖涓獀m涓婄殑绉熺敤鑺辫垂
			if(increasedCost < minIncreasedCost){ 
				minIncreasedCost = increasedCost;
				selectedVM = vm;
				selectedStartTime = startTime;
			}
		}

		//test whether a new VM can meet the sub-deadline and (or) reduce increasedCost; if so, add this new VM
		int selectedI = -1;				
		startTime = taskIndex==1 ? VM.LAUNCH_TIME : solution.calcEST(task, null);//鏍规嵁鏄惁鏄涓�涓换鍔★紝鍐冲畾寮�濮嬫椂闂�
		/*for(int k = 0 ; k<VM.TYPE_NO; k++){
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[k];//鍦ㄧk涓櫄鎷熸満涓婄殑缁撴潫鏃堕棿
			if(finishTime > subDeadline + Evaluate.E)	//sub-deadline not met
				continue;
			
			double increasedCost = Math.ceil((finishTime - startTime)/VM.INTERVAL) * VM.UNIT_COSTS[k];
			if(increasedCost < minIncreasedCost){//鑻ユ湁姣斾箣鍓嶉�夋嫨鐨勮櫄鎷熸満澧為暱璐圭敤杩樺皯鐨勶紝鍒欑粰鍒拌铏氭嫙鏈�
				minIncreasedCost = increasedCost;
				selectedI = k;
				selectedStartTime = startTime;
			}
		}*/
		if(selectedI != -1)
			selectedVM = new VM(selectedI);
		if(selectedVM == null)
			return null;
		else//鍙涓嶆槸null锛屽氨杩斿洖璋冨害Allocation
			return new Allocation(selectedVM, task, selectedStartTime);
	}
	
	private Allocation getProperVM(Task task, Solution solution, double subDeadline, double CTB, int taskIndex , List<VM> aType) {
		VM selectedVM = null;
		double selectedStartTime = 0;
		double earliestFinishTime = subDeadline;
		//double limitCost = CTB;
		double maxOutTime = 0;	//maxTransferOutTime
		for(Edge e : task.getOutEdges())
			maxOutTime = Math.max(maxOutTime, e.getDataSize());
		maxOutTime /= VM.NETWORK_SPEED;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that meets sub-deadline and minimizes the cost
		for(VM vm : aType){
			startTime = solution.calcEST(task, vm); 
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			if(finishTime < earliestFinishTime) {
				earliestFinishTime = finishTime;
				selectedVM = vm;
				selectedStartTime = startTime;
			}
			//if(finishTime > subDeadline + Evaluate.E)   //sub-deadline not met
				//continue;
			//newVMPeriod 绉熺敤鏃堕暱
			//double newVMPeriod = finishTime + maxOutTime - solution.getVMLeaseStartTime(vm);//finishTime + maxOutTime==LFT(Lease finish Time) 锛�9锛�
			//double newVMTotalCost = Math.ceil(newVMPeriod/VM.INTERVAL) * vm.getUnitCost();//EC
			//double increasedCost = newVMTotalCost;// - solution.calcVMCost(vm);  // oldVMTotalCost //鍚庡崼鍘熸湰solution涓繖涓獀m涓婄殑绉熺敤鑺辫垂
			/*if(increasedCost < limitCost){ 
				limitCost = increasedCost;
				selectedVM = vm;
				selectedStartTime = startTime;
			}*/
		}
		int selectedI = -1;				
		startTime = taskIndex==1 ? VM.LAUNCH_TIME : solution.calcEST(task, null);//鏍规嵁鏄惁鏄涓�涓换鍔★紝鍐冲畾寮�濮嬫椂闂�
		/*for(int k = 0 ; k<VM.TYPE_NO; k++){
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[k];//鍦ㄧk涓櫄鎷熸満涓婄殑缁撴潫鏃堕棿
			//if(finishTime > subDeadline + Evaluate.E)	//sub-deadline not met
				//continue;
			
			double increasedCost = Math.ceil((finishTime - startTime)/VM.INTERVAL) * VM.UNIT_COSTS[k];
			if(increasedCost < limitCost){//鑻ユ湁姣斾箣鍓嶉�夋嫨鐨勮櫄鎷熸満澧為暱璐圭敤杩樺皯鐨勶紝鍒欑粰鍒拌铏氭嫙鏈�
				limitCost = increasedCost;
				selectedI = k;
				selectedStartTime = startTime;
			}
		}*/
		if(selectedI != -1)
			selectedVM = new VM(selectedI);
		if(selectedVM == null)
			return null;
		else
		    return new Allocation(selectedVM, task, selectedStartTime);
	}
}
