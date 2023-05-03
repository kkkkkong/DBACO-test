package cloud.workflowScheduling.methods;

import java.util.*;

import cloud.workflowScheduling.*;
import cloud.workflowScheduling.setting.*;

public class DBAllin2 implements Scheduler{
    private final double bestVMSpeed = VM.SPEEDS[VM.FASTEST];
    double deadline;
    double RB;//Budget
    double CPLength;
	//public Solution solution = new Solution();
	private double theta = 2;
//	private Map<Task, Double> leastCosts;
//	private Map<Task, Map<Integer, Double>> standardCosts; 
	
	private Map<Task, Double> actualCost;
//	private List<Task> allocated;
//	private List<Task> unallocated;
//	private List<VM> aType;
	
	private Map<Double, List<Task>> BOT;
	private Map<Task, Double> level;
	private Map<Double, Double> level_subB;
	private Map<Task, Double> task_subD;
	private Map<Task, Map<VM, Double>> increaseCosts;
	private Map<Task, Map<VM, Double>> costFactors;
	//private Map<Task, Map<Integer, Double>> typeCostFactors;
	private Map<Task, Map<VM, Double>> timeFactors;
	//private Map<Task, Map<Integer, Double>> typeTimeFactors;
	private Map<Task, Map<VM, Double>> tradeOffFactors;
	//private Map<Task, Map<Integer, Double>> typeTradeOffFactors;
	
	//public Solution solution = new Solution();
	
	public DBAllin2(double theta) {
		this.theta = theta;
	}
	public double getTheta() {
		return theta;
	}
	
	public Solution schedule(Workflow wf) {
		//System.out.println("&&SOlution:"+ solution);
		wf.calcPURank(theta);
		List<Task> wfTasks = new ArrayList<Task>(wf);
		
		RB = wf.getBudget();
		deadline = wf.getDeadline();
		Task entry = wf.get(0);
		Task exit = wf.get(wf.size()-1);
		BOT = new HashMap<>();
		level = new HashMap<>();
		level_subB = new HashMap<>();
		task_subD = new HashMap<>();
		
		increaseCosts = new HashMap<>();
		costFactors = new HashMap<>();
		timeFactors = new HashMap<>();
		tradeOffFactors = new HashMap<>();
		
		actualCost = new HashMap<>();

		calculateTaskSubD(wfTasks, deadline);
		calculateTasksEST(wfTasks);
		calculateLevels(wfTasks);
		calculateBagOfTasks(wfTasks);
		
		int maxLevel = BOT.size();
		level_subB.put((double)(maxLevel-1), RB);
		level_subB.put((double)(maxLevel-2), RB);
		printBoT();

		return allocateBoTTasks(entry, exit);
		//return buildViaTaskList(wf, wfTasks, wf.getDeadline());
	}
	
		
	private void calculateTaskSubD(List<Task> wfTasks, Double deadline) {
		double CPLength = wfTasks.get(0).getpURank();
		Task entry = wfTasks.get(0);
		task_subD.put(entry, deadline);
		for(int i = 1; i < wfTasks.size(); i++) {
			Task task = wfTasks.get(i);
			double subDeadline = (CPLength - task.getpURank() + task.getTaskSize()/VM.SPEEDS[VM.FASTEST])
					/CPLength * deadline;
			task_subD.put(task, subDeadline);
		}
	}
	
	//璁＄畻宸ヤ綔娴佷腑姣忎釜浠诲姟鐨勬渶鏃╁紑濮嬫椂闂碋ST
    private void calculateTasksEST(List<Task> wfTasks) {
		Task entryTask = wfTasks.get(0);
		entryTask.setAST(0);
		entryTask.setAFT(0);
		entryTask.setAssigned(true);
		
		for(int i=1; i<wfTasks.size(); i++){		// compute EST, EFT, critical parent via Eqs. 1 and 2; skip entry task
			Task task = wfTasks.get(i);

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
    
    private void calculateLevels(List<Task> tasks) {
    	for(Task task : tasks) {
    		calculateLevel(task);
    	}
    }
    
    private void calculateLevel(Task task) {
    	//浠庢牴缁撶偣寮�濮嬪悜涓婂畾绾�
    	if(!level.containsKey(task)) {
    		List<Edge> outE = task.getOutEdges();
    		if(outE.size() == 0) {
    			level.put(task, 0.0);
    			task.settLevel(0.0);
    			System.out.println("# Level:" + task +"#is#" + 0.0);
    			return;
    		}
    	double max_level = 0;
    	
    	for(Edge e : task.getOutEdges()) {
    		Task succ = e.getDestination();
    		if(!level.containsKey(succ)) {
    			calculateLevel(succ);
    		}
    	}
    	for(Edge e : task.getOutEdges()) {
    		Task succ = e.getDestination();
    		max_level = Math.max(max_level, level.get(succ));
    	}
    	level.put(task, max_level+1.0);
    	task.settLevel(max_level+1.0);
    	System.out.println("# Level:" + task +"#is#" + (max_level+1.0));
    	}
    	
    }
    
    private void calculateBagOfTasks(List<Task> tasks) {
    	for(Task task : tasks) {
    		if(!BOT.containsKey(level.get(task))) {
    			List<Task> ltasks = new ArrayList<Task>();
    			ltasks.add(task);
    			BOT.put(level.get(task), ltasks);
    		}else {
    			List<Task> ltasks = BOT.get(level.get(task));
    			ltasks.add(task);
    			BOT.put(level.get(task), ltasks);
    			//姝ゅ鐨刾ut浼氳鐩栧師鍏堢殑latasks
    		}
    	}
    }
    
    private void printBoT() {
    	int len = BOT.size();
    	for(int i = 1; i<len; i++) {
    		List<Task> ltasks = BOT.get((double)i);
    		
    		Collections.sort(ltasks, new Comparator<Task>() {
    			@Override
    			public int compare(Task o1, Task o2) {
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
    		for(Task task : ltasks) {
    			//System.out.println("task : " + task.getId() + " #" + "EST is #" + task.getEST() + " #");
    		}
    	}
    }
    
    private Solution allocateBoTTasks(Task entry, Task exit) {
    	Solution solution = new Solution();
    	int len = BOT.size();
    	System.out.println("Previous Solution : " + solution);
    	
    	//solution.getRevMapping().clear();
    	Allocation alloc = getMinEFTVM(entry, solution, deadline);
    	solution.addTaskToVM(alloc.getVM(), entry, alloc.getStartTime(), true);
    	
    	System.out.println("Current Solution : " + solution);
    	
    	for(int i = len-2; i >=0; i--) {
    		List<Task> ltasks = BOT.get((double)i);
    		allocateBotTask(ltasks, solution);
    	}
    	return solution;
    }
    
    Solution allocateBotTask(List<Task> tasks, Solution solution) {
    	int violationCount = 0;
    	double sumCost = 0.0;
    	Task oneToGetLevel = tasks.get(0);
    	double currentLevel = oneToGetLevel.gettLevel();
    	//鑾峰緱褰撳墠level鐨剆ubB
    	double subB = level_subB.get(currentLevel);
    	System.out.println("Current Level Budget: " + subB);
    	for(int i = 0; i < tasks.size(); i++) {
    		Task task = tasks.get(i);
    		double subDeadline = task_subD.get(task);
    		
    		double maxOutTime = 0;
    		for(Edge e : task.getOutEdges()){
    			maxOutTime = Math.max(maxOutTime, e.getDataSize());
    		}
	    	Allocation alloc;
    		//calculateFactors(task, subB, solution);
    		if(subB<=0) {
    			alloc = getExistedVM(task, solution, subDeadline);
    		}else {
    			alloc = getBalanceVM(task, subB, solution);
    		}
    		if(alloc==null) {
    			alloc = getMinEFTVM(task,solution, subDeadline);
    			VM vm = alloc.getVM();
    			while(alloc.getFinishTime() > subDeadline + Evaluate.E && vm.getType() < VM.FASTEST) {
    				solution.updateVM(vm);
    				alloc.setStartTime(solution.calcEST(task, vm));
    				alloc.setFinishTime(solution.calcEST(task, vm)+task.getTaskSize()/vm.getSpeed());
    			}
    			double oldC = Math.ceil(alloc.getStartTime()-VM.LAUNCH_TIME)/VM.INTERVAL * vm.getUnitCost();
    			double newC = solution.calcVMCost(vm);
    			double increase = newC - oldC;
    			Map<VM, Double> vmToIncrease = new HashMap<>();
    			vmToIncrease.put(vm, increase);
    			increaseCosts.put(task, vmToIncrease);
    		}
    		if(alloc.getFinishTime() > subDeadline + Evaluate.E) {
    			violationCount++;
    		}
    		
    		
    		solution.addTaskToVM(alloc.getVM(), task, alloc.getStartTime(), true);
    		//double currentCost = (alloc)
    		//double currentCost = ((task.getTaskSize()/alloc.getVM().getSpeed()+maxOutTime)/VM.INTERVAL) *alloc.getVM().getUnitCost();
    		double currentCost = increaseCosts.get(task).get(alloc.getVM());
    		actualCost.put(task, currentCost);
    		sumCost +=currentCost;
    		if(i==tasks.size()-1)
    			level_subB.put(currentLevel-1.0, subB-sumCost);
    	}
    	return solution;
    }
    

    
    //鏁村悎FactorCalculate鍜実etVM
    //鍦ㄤ竴涓柟娉曚腑杩涜
    private Allocation getBalanceVM(Task task, Double subB, Solution solution) {
    	
    	double costFactor;
    	double timeFactor;
    	double tradeOffFactor;
    	Map<VM, Double> vmToIncreaseCost = new HashMap<>();
    	Map<VM, Double> vmToCost = new HashMap<>();
    	Map<Integer, Double> typeToCost = new HashMap<>();
    	Map<VM, Double> vmToCostFactor = new HashMap<>();
    	Map<Integer, Double> typeToCostFactor = new HashMap<>();
    	Map<VM, Double> vmToTimeFactor = new HashMap<>();
    	Map<Integer, Double> typeToTimeFactor = new HashMap<>();
    	Map<VM, Double> vmToTradeOffFactor = new HashMap<>();
    	Map<Integer, Double> typeToTradeOffFactor = new HashMap<>();
    	
    	double startTime, finishTime;
    	double maxECT = 0;
    	double minECT = Double.MAX_VALUE;
    	double currentECT;
    	
    	double bestCost = Double.MAX_VALUE;
    	
    	
    	double maxOutTime=0;
    	for(Edge e : task.getOutEdges()) {
    		maxOutTime = Math.max(maxOutTime, e.getDataSize());
    	}
    	maxOutTime /= VM.NETWORK_SPEED;
    	
    	for(VM vm : solution.keySet()) {
    		startTime = solution.calcEST(task, vm);
    		finishTime = startTime + task.getTaskSize()/vm.getSpeed();
    		if(minECT > (finishTime - startTime))
    			minECT = (finishTime - startTime);
    		if(maxECT < (finishTime - startTime))
    			maxECT = (finishTime - startTime);
    		double newVMPeriod = finishTime + maxOutTime -solution.getVMLeaseStartTime(vm);
    	    double newVMTotalCost = Math.ceil(newVMPeriod/VM.INTERVAL) * vm.getUnitCost();
    	    double increaseCost = newVMTotalCost - solution.calcVMCost(vm);
    	    vmToIncreaseCost.put(vm, increaseCost);
    	    vmToCost.put(vm, increaseCost);
    	    if(bestCost > increaseCost)
    	    	bestCost = increaseCost;
    	}
    	//increaseCosts.put(task, vmToIncreaseCost);
    	
    	startTime = solution.calcEST(task, null);
    	for(int k=0; k<VM.TYPE_NO; k++) {
    		finishTime = startTime + task.getTaskSize()/VM.SPEEDS[k];
    		double increaseCost = Math.ceil((finishTime - startTime)/VM.INTERVAL);
    		typeToCost.put(k, increaseCost);
    		
    		if(bestCost > increaseCost)
    			bestCost = increaseCost;
    	}
    	
    	for(VM vm : solution.keySet()) {
    		startTime = solution.calcEST(task, vm);
    		finishTime = startTime + task.getTaskSize()/vm.getSpeed();
    		currentECT = finishTime - startTime;
    		if(subB == 0) {
    			vmToCostFactor.put(vm, 0.0);
    			vmToTradeOffFactor.put(vm, 0.0);
    		}else {
    			costFactor = (subB - vmToCost.get(vm))/(subB - bestCost);
    			vmToCostFactor.put(vm, costFactor);
    			
    			timeFactor = (maxECT - currentECT)/(maxECT - minECT);
    			if(maxECT==currentECT)
    				timeFactor = 0.0;
    			vmToTimeFactor.put(vm, timeFactor);
    			tradeOffFactor = timeFactor/costFactor;
    			vmToTradeOffFactor.put(vm, tradeOffFactor);
    		}
    	}
    	costFactors.put(task, vmToCostFactor);
    	timeFactors.put(task, vmToTimeFactor);
    	tradeOffFactors.put(task, vmToTradeOffFactor);
    	
    	startTime = solution.calcEST(task, null);
    	for(int k=0; k<VM.TYPE_NO; k++) {
    		currentECT = task.getTaskSize()/VM.SPEEDS[k];
    		if(subB==0) {
    			typeToCostFactor.put(k, 0.0);
    			typeToTradeOffFactor.put(k, 0.0);
    		}else {
    			costFactor = (subB - typeToCost.get(k))/(subB - bestCost);
    			typeToCostFactor.put(k, costFactor);
    			
    			timeFactor = (maxECT - currentECT)/(maxECT - minECT);
    			if(maxECT == currentECT)
    				timeFactor=0.0;
    			typeToTimeFactor.put(k, timeFactor);
    			tradeOffFactor = timeFactor/costFactor;
    			
    			typeToTradeOffFactor.put(k, tradeOffFactor);
    		}
    	}
    	//System.out.println("SHIT"+typeToCostFactor);
    	//typeCostFactors.put(task, typeToCostFactor);
    	//typeTimeFactors.put(task, typeToTimeFactor);
    	//typeTradeOffFactors.put(task, typeToTradeOffFactor);
    	
    	
    	VM selectedVM = null;
    	double selectedStartTime =0;
    	
    	double minFactor = Double.MAX_VALUE;
    	double differ;
    	int selectedI = -1;
    	//1.选择与1最相近trade off factor的
//    	
//    	
    	for(VM vm : solution.keySet()) {
    		tradeOffFactor = vmToTradeOffFactor.get(vm);
    		differ = Math.abs(1-tradeOffFactor);
    		if(differ < minFactor) {
    			minFactor = differ;
    			selectedVM = vm;
    			selectedStartTime = solution.calcEST(task, selectedVM);
    		}
    		//System.out.println("##MinFactor:"+minFactor);
    	}
    	for(int k=0; k< VM.TYPE_NO; k++) {
    		tradeOffFactor = typeToTradeOffFactor.get(k);
    		differ = Math.abs(1-tradeOffFactor);
    		//System.out.println("##Differ:" + differ);
    		if(differ <= minFactor) {
    			minFactor = differ;
    			selectedI = k;
    			selectedStartTime = solution.calcEST(task, null);
    		}
    		//System.out.println("##SelectedI:"+selectedI);
    	}
    	
    	//2.选择最大的
//    	double maxFactor=0;
//    	for(VM vm : solution.keySet()) {
//    		tradeOffFactor = vmToTradeOffFactor.get(vm);
//    		if(tradeOffFactor > maxFactor) {
//    			maxFactor = tradeOffFactor;
//    			selectedVM = vm;
//    			selectedStartTime = solution.calcEST(task, selectedVM);
//    	}
//	}
//    	for(int k=0; k<VM.TYPE_NO; k++) {
//    		tradeOffFactor = typeToTradeOffFactor.get(k);
//    		if(tradeOffFactor > maxFactor) {
//    			maxFactor = tradeOffFactor;
//    			selectedI = k;
//    			selectedStartTime = solution.calcEST(task, null);
//    		}
//    	}
    	
    	//3.选择最小的
//    	for(VM vm : solution.keySet()) {
//    		tradeOffFactor = vmToTradeOffFactor.get(vm);
//    		if(tradeOffFactor < minFactor) {
//			   minFactor = tradeOffFactor;
//			   selectedVM = vm;
//			   selectedStartTime = solution.calcEST(task, selectedVM);
//		    }
//    	}
//    	
//    	for(int k=0; k<VM.TYPE_NO; k++) {
//    		tradeOffFactor = typeToTradeOffFactor.get(k);
//    		if(tradeOffFactor < minFactor) {
//    			minFactor = tradeOffFactor;
//    			selectedI = k;
//    			selectedStartTime = solution.calcEST(task, null);
//    		}
//    	}
    	
    	if(selectedI != -1) {
    		selectedVM = new VM(selectedI);
    		vmToIncreaseCost.put(selectedVM, typeToCost.get(selectedI));
    		
    	}
    	increaseCosts.put(task, vmToIncreaseCost);
    	
    	if(selectedVM == null)
    		return null;
    	else
    		return new Allocation(selectedVM, task, selectedStartTime);
    }
    

	
	private Allocation getMinEFTVM(Task task, Solution solution, double subDeadline){
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
	
	private Allocation getMinCostVM(Task task, Solution solution, double subDeadline){
		double minIncreasedCost = Double.MAX_VALUE;	//increased cost for one VM is used here, instead of total cost
		VM selectedVM = null;
		double selectedStartTime = 0;
		
		double maxOutTime = 0;	//maxTransferOutTime
		for(Edge e : task.getOutEdges())//OutEdges:浠巘ask鍑哄彂杩炲悜鍏朵粬浠诲姟鐨勮竟
			maxOutTime = Math.max(maxOutTime, e.getDataSize());//閬嶅巻鍙栧緱鏈�澶х殑鍑鸿竟涓婄殑鏁版嵁澶у皬
		maxOutTime /= VM.NETWORK_SPEED;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that meets sub-deadline and minimizes the cost
		if(solution.size()!=0) {
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
		}

		//test whether a new VM can meet the sub-deadline and (or) reduce increasedCost; if so, add this new VM
		int selectedI = -1;				
		startTime = solution.calcEST(task, null);//鏍规嵁鏄惁鏄涓�涓换鍔★紝鍐冲畾寮�濮嬫椂闂�
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
	private Allocation getExistedVM(Task task, Solution solution, double subDeadline){
		double minIncreasedCost = Double.MAX_VALUE;	//increased cost for one VM is used here, instead of total cost
		VM selectedVM = null;
		double selectedStartTime = 0;
		Map<VM, Double> increaseCost = new HashMap<>();
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
		if(selectedVM == null)
			return null;
		increaseCost.put(selectedVM, minIncreasedCost);
		increaseCosts.put(task, increaseCost);
		return new Allocation(selectedVM, task, selectedStartTime);
	}
	

}
