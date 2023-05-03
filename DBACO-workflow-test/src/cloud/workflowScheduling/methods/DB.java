package cloud.workflowScheduling.methods;

import java.util.*;

import cloud.workflowScheduling.*;
import cloud.workflowScheduling.setting.*;

public class DB implements Scheduler{
    private final double bestVMSpeed = VM.SPEEDS[VM.FASTEST];
    double deadline;
    double RB;
    double CPLength;
	//public Solution solution = new Solution();
	private double theta = 2;
	private Map<Task, Double> averageCosts;
	private Map<Task, Double> leastCosts;
	private Map<Task, Map<Integer, Double>> standardCosts; 
	private Map<Task, Double> actualCost;
	private List<Task> allocated;
	private List<Task> unallocated;
	private List<VM> aType;
	
	public DB(double theta) {
		this.theta = theta;
	}
	public double getTheta() {
		return theta;
	}
	
	public Solution schedule(Workflow wf) {
		wf.calcPURank(theta);
		List<Task> wfTasks = new ArrayList<Task>(wf);
		Collections.sort(wfTasks, new Task.PURankComparator());
		Collections.reverse(wfTasks);
		
		averageCosts = new HashMap<>();
		leastCosts = new HashMap<>();
		standardCosts = new HashMap<>();
		actualCost = new HashMap<>();
		allocated = new ArrayList<>();
		unallocated = new ArrayList<>();
		aType = new ArrayList<>();
		
		return buildViaTaskList(wf, wfTasks, wf.getDeadline());
		
	}
	
	Solution buildViaTaskList(Workflow wf, List<Task> wfTasks, double deadline) {
		deadline = wf.getDeadline();
		RB = wf.getBudget();
		leastCosts = new HashMap<>();
		averageCosts = new HashMap<>();
		standardCosts = new HashMap<>();
		actualCost = new HashMap<>();
		allocated = new ArrayList<>();
		unallocated = new ArrayList<>();
		aType = new ArrayList<>();
		
		//System.out.println("##鎴鏈熼檺涓猴細" + deadline);
		//System.out.println("PDBCAC's Budget: " + RB);

		
		
		calculateTasksEST(wfTasks);
		calculateCost(wfTasks);

		return allocateTasks(wf, wfTasks, wf.getDeadline());
	}
	//璁＄畻宸ヤ綔娴佷腑姣忎釜浠诲姟鐨勬渶鏃╁紑濮嬫椂闂碋ST
    private void calculateTasksEST(List<Task> wfTasks) {
		Task entryTask = wfTasks.get(0);
		entryTask.setAST(0);
		entryTask.setAFT(0);
		entryTask.setAssigned(true);
		
		for(int i=1; i<wfTasks.size(); i++){		// compute EST, EFT, critical parent via Eqs. 1 and 2; skip entry task
			Task task = wfTasks.get(i);

			//锟剿达拷EST锟斤拷锟藉不锟斤拷锟斤拷resource锟斤拷available time锟斤拷锟揭伙拷要锟斤拷锟斤拷critical parent锟斤拷锟斤拷锟斤拷没锟斤拷使锟斤拷solution.calcEST锟斤拷锟斤拷
			//锟斤拷锟斤拷锟斤拷锟斤拷EST锟斤拷EFT锟斤拷锟节碉拷锟斤拷之前锟斤拷
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
		Task exitTask = wfTasks.get(wfTasks.size()-1);	//Note, EST, EFT, critialParent of exitTask have been set above
		exitTask.setAFT(deadline);
		exitTask.setAST(deadline);
	}
    
    //Calculate the AverageCost and StandardCost for each task on VM of different type
    private void calculateCost(List<Task> tasks) {
    	for(Task task : tasks) {
    		double averageCost = 0;
    		double maxOutTime = 0;
    		double leastCost = Double.MAX_VALUE;
    		Map<Integer, Double> typeToCost = new HashMap<>();
    		for(int i=0;i<VM.TYPE_NO;i++) {
    			typeToCost.put(i, 0.0);
    		}
    		standardCosts.put(task, typeToCost);
    		for(Edge e : task.getOutEdges())
    			maxOutTime = Math.max(maxOutTime, e.getDataSize());
    		maxOutTime /=VM.NETWORK_SPEED;
    		
    		for(int i=0;i<VM.TYPE_NO;i++) {
    			double cost = Math.ceil((task.getTaskSize()/VM.SPEEDS[i]+maxOutTime)/VM.INTERVAL) * VM.UNIT_COSTS[i];
    			averageCost +=Math.ceil((task.getTaskSize()/VM.SPEEDS[i]+maxOutTime)/VM.INTERVAL) * VM.UNIT_COSTS[i];
    			typeToCost.put(i, Math.ceil((task.getTaskSize()/VM.SPEEDS[i]+maxOutTime)/VM.INTERVAL) * VM.UNIT_COSTS[i]);
    			standardCosts.put(task, typeToCost);
				leastCost = Math.min(leastCost, cost);
    		}
    		averageCost /=VM.TYPE_NO;
    		averageCosts.put(task, averageCost);
    		leastCosts.put(task, leastCost);
    	}
    }
    

    
    

	
	 Solution allocateTasks(Workflow wf, List<Task> wfTasks, double deadline) {
		Solution solution = new Solution();

		for(Task task : wfTasks) {
			unallocated.add(task);
		}
		
		for(int i=1; i<wfTasks.size(); i++) {
			double CPLength = wf.get(0).getpURank();
			Task task = wfTasks.get(i);
			aType.clear();
			
			double proSubDeadline = (CPLength - task.getpURank() + task.getTaskSize()/VM.SPEEDS[VM.FASTEST])
					/CPLength * deadline;
			Allocation alloc = allocateTask(task, solution, proSubDeadline, i);
			
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
		double TSB=0.0;
		double CTB=0.0;
		double allocatedSum=0.0;
    	double unallocatedSum=0.0;
		
		for(Task proceed : allocated) {
			allocatedSum += actualCost.get(proceed); 
		}
		for(Task procedent : unallocated) {
			unallocatedSum +=leastCosts.get(procedent);
		}
		
		TSB = RB - allocatedSum - unallocatedSum;
		
		
		if(TSB >= 0) {
    		CTB = averageCosts.get(task)+ TSB * averageCosts.get(task)/unallocatedSum;
    	}else CTB = averageCosts.get(task);
		
		for(VM vm : solution.keySet()) {
			if(standardCosts.get(task).get(vm.getType())<CTB) {
				aType.add(vm);
			}
		}
		//Case1
		if(!aType.isEmpty()) {
			alloc = getProperVM(task, solution, proSubDeadline, CTB, taskIndex, aType);
			if(alloc == null)
				alloc = getMinEFTVM(task, solution, proSubDeadline, taskIndex);    
		}
		//Case2
		else if(TSB>=0) {
			//alloc = getMinEFTVM(task, solution, proSubDeadline, taskIndex);
			alloc = getMinCostVM(task, solution, proSubDeadline, taskIndex);
			if(alloc == null)
				alloc = getMinEFTVM(task, solution, proSubDeadline, taskIndex);
			
		}else {
			alloc = getMinCostVM(task, solution, proSubDeadline, taskIndex);
			if(alloc == null){//sl涓虹┖			//select a vm which allows EFT
				alloc = getMinEFTVM(task, solution, proSubDeadline, taskIndex);//閫夋嫨璇ユ儏鍐典笅鏈�蹇殑vm
			}
		}
		VM vm = alloc.getVM();
		while(alloc.getFinishTime() > proSubDeadline + Evaluate.E && vm.getType() < VM.FASTEST){//褰撳墠鏈�蹇殑閮借秴杩囧瓙鎴鏃堕棿锛屼笖涓嶆槸鏈�蹇瓑绾х殑锛屽垯鍗囩骇
		    solution.updateVM(vm);			//upgrade锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷母锟斤拷拢锟斤拷锟斤拷佣冉锟斤拷锟斤拷锟教拷唷�
		    alloc.setStartTime(solution.calcEST(task, vm));
		    alloc.setFinishTime(solution.calcEST(task, vm) + task.getTaskSize()/vm.getSpeed());
	}
		double deltC = CTB-(Math.ceil(alloc.getFinishTime()/VM.INTERVAL) * vm.getUnitCost()
				-Math.ceil(alloc.getStartTime()/VM.INTERVAL) * vm.getUnitCost());
		System.out.println(task + "Delta Cost:" + deltC);
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
			if(finishTime > subDeadline + Evaluate.E)   //sub-deadline not met
				continue;
			//newVMPeriod 绉熺敤鏃堕暱
			double newVMPeriod = finishTime + maxOutTime - solution.getVMLeaseStartTime(vm);//finishTime + maxOutTime==LFT(Lease finish Time) 锛�9锛�
			double newVMTotalCost = Math.ceil(newVMPeriod/VM.INTERVAL) * vm.getUnitCost();//EC
			double increasedCost = newVMTotalCost - solution.calcVMCost(vm);  // oldVMTotalCost //鍚庡崼鍘熸湰solution涓繖涓獀m涓婄殑绉熺敤鑺辫垂
			//閫氳繃瀵箆m鐨勮凯浠ｏ紝鑾峰緱浣垮緱鎬荤鐢ㄨ姳璐瑰闀跨殑铏氭嫙鏈�
			if(increasedCost < minIncreasedCost){ 
				minIncreasedCost = increasedCost;
				selectedVM = vm;
				selectedStartTime = startTime;
			}
		}

		//test whether a new VM can meet the sub-deadline and (or) reduce increasedCost; if so, add this new VM
		int selectedI = -1;				
		startTime = taskIndex==1 ? VM.LAUNCH_TIME : solution.calcEST(task, null);//鏍规嵁鏄惁鏄涓�涓换鍔★紝鍐冲畾寮�濮嬫椂闂�
		for(int k = 0 ; k<VM.TYPE_NO; k++){
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[k];//鍦ㄧk涓櫄鎷熸満涓婄殑缁撴潫鏃堕棿
			if(finishTime > subDeadline + Evaluate.E)	//sub-deadline not met
				continue;
			
			double increasedCost = Math.ceil((finishTime - startTime)/VM.INTERVAL) * VM.UNIT_COSTS[k];
			if(increasedCost < minIncreasedCost){//鑻ユ湁姣斾箣鍓嶉�夋嫨鐨勮櫄鎷熸満澧為暱璐圭敤杩樺皯鐨勶紝鍒欑粰鍒拌铏氭嫙鏈�
				minIncreasedCost = increasedCost;
				selectedI = k;
				selectedStartTime = startTime;
			}
		}
		//鑻ユ壘鍒颁簡锛屽垯浠樼粰璇m
		if(selectedI != -1)
			selectedVM = new VM(selectedI);
		//鑻ユ病鏈夊垯杩斿洖null
		if(selectedVM == null)
			return null;
		else//鍙涓嶆槸null锛屽氨杩斿洖璋冨害Allocation
			return new Allocation(selectedVM, task, selectedStartTime);
	}


	private Allocation getProperVM(Task task, Solution solution, double subDeadline, double CTB, int taskIndex, List<VM> aType) {
		VM selectedVM = null;
		double selectedStartTime = 0;
		double limitCost = CTB;
		double maxOutTime = 0;	//maxTransferOutTime
		for(Edge e : task.getOutEdges())
			maxOutTime = Math.max(maxOutTime, e.getDataSize());
		maxOutTime /= VM.NETWORK_SPEED;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that meets sub-deadline and minimizes the cost
		for(VM vm : aType){
			startTime = solution.calcEST(task, vm); 
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			if(finishTime > subDeadline + Evaluate.E)   //sub-deadline not met
				continue;
			//newVMPeriod 绉熺敤鏃堕暱
			double newVMPeriod = finishTime + maxOutTime - solution.getVMLeaseStartTime(vm);//finishTime + maxOutTime==LFT(Lease finish Time) 锛�9锛�
			double newVMTotalCost = Math.ceil(newVMPeriod/VM.INTERVAL) * vm.getUnitCost();//EC
			double increasedCost = newVMTotalCost - solution.calcVMCost(vm);  // oldVMTotalCost //鍚庡崼鍘熸湰solution涓繖涓獀m涓婄殑绉熺敤鑺辫垂
			//閫氳繃瀵箆m鐨勮凯浠ｏ紝鑾峰緱浣垮緱鎬荤鐢ㄨ姳璐瑰闀跨殑铏氭嫙鏈�
			if(increasedCost < limitCost){ 
				limitCost = increasedCost;
				selectedVM = vm;
				selectedStartTime = startTime;
			}
		}
		int selectedI = -1;				
		startTime = taskIndex==1 ? VM.LAUNCH_TIME : solution.calcEST(task, null);//鏍规嵁鏄惁鏄涓�涓换鍔★紝鍐冲畾寮�濮嬫椂闂�
		for(int k = 0 ; k<VM.TYPE_NO; k++){
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[k];//鍦ㄧk涓櫄鎷熸満涓婄殑缁撴潫鏃堕棿
			if(finishTime > subDeadline + Evaluate.E)	//sub-deadline not met
				continue;
			
			double increasedCost = Math.ceil((finishTime - startTime)/VM.INTERVAL) * VM.UNIT_COSTS[k];
			if(increasedCost < limitCost){//鑻ユ湁姣斾箣鍓嶉�夋嫨鐨勮櫄鎷熸満澧為暱璐圭敤杩樺皯鐨勶紝鍒欑粰鍒拌铏氭嫙鏈�
				limitCost = increasedCost;
				selectedI = k;
				selectedStartTime = startTime;
			}
		}
		//鑻ユ壘鍒颁簡锛屽垯浠樼粰璇m
		if(selectedI != -1)
			selectedVM = new VM(selectedI);
		//鑻ユ病鏈夊垯杩斿洖null
		if(selectedVM == null)
			return null;
		else//鍙涓嶆槸null锛屽氨杩斿洖璋冨害Allocation
		return new Allocation(selectedVM, task, selectedStartTime);
	}
}
