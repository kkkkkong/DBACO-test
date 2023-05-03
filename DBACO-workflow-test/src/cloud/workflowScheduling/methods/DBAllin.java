package cloud.workflowScheduling.methods;

import java.util.*;

import cloud.workflowScheduling.*;
import cloud.workflowScheduling.setting.*;

public class DBAllin implements Scheduler{
    private final double bestVMSpeed = VM.SPEEDS[VM.FASTEST];
    double deadline;
    double RB;//Budget
    double CPLength;
	//public Solution solution = new Solution();
	private double theta = 2;
	private Map<Task, Double> leastCosts;
	private Map<Task, Map<Integer, Double>> standardCosts; 
	
	private Map<Task, Double> actualCost;
	private List<Task> allocated;
	private List<Task> unallocated;
	private List<VM> aType;
	
	private Map<Double, List<Task>> BOT;
	private Map<Task, Double> level;
	private Map<Double, Double> level_subB;
	private Map<Task, Double> task_subD;
	private Map<Task, Map<VM,Double>> increaseCosts;
	private Map<Task, Map<VM, Double>> costFactors;
	private Map<Task, Map<VM, Double>> timeFactors;
	private Map<Task, Map<VM, Double>> tradeOffFactors;
	//public Solution solution = new Solution();
	
	public DBAllin(double theta) {
		this.theta = theta;
	}
	public double getTheta() {
		return theta;
	}
	
	public Solution schedule(Workflow wf) {
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
		level_subB.put((double)maxLevel-1, RB);
		level_subB.put((double)maxLevel-2, RB);
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
	
	//计算工作流中每个任务的最早开始时间EST
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
    	//从根结点开始向上定级
    	if(!level.containsKey(task)) {
    		List<Edge> outE = task.getOutEdges();
    		if(outE.size() == 0) {
    			level.put(task, 0.0);
    			task.settLevel(0.0);
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
    			//此处的put会覆盖原先的latasks
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
    			System.out.println("task : " + task.getId() + " #" + "EST is #" + task.getEST() + " #");
    		}
    	}
    }
    
    private Solution allocateBoTTasks(Task entry, Task exit) {
    	Solution solution = new Solution();
    	int len = BOT.size();
    	System.out.println("Previous Solution : " + solution);
    	Allocation alloc = getMinCostVM(entry, solution, deadline);
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
    	//获得当前level的subB
    	double subB = level_subB.get(currentLevel);
    	System.out.println("Current Level Budget: " + subB);
    	for(int i = 0; i < tasks.size(); i++) {
    		Task task = tasks.get(i);
    		double subDeadline = task_subD.get(task);
    		
    		double maxOutTime = 0;
    		for(Edge e : task.getOutEdges()){
    			maxOutTime = Math.max(maxOutTime, e.getDataSize());
    		}
	    	
    		calculateFactors(task, subB, solution);
    		
    		Allocation alloc = getBalanceVM(task, solution);
    		
    		if(alloc.getFinishTime() > subDeadline + Evaluate.E) {
    			violationCount++;
    		}
    		
    		/*if(alloc == null) {
    			System.out.println(task.getId() + "Need Alloc");
    			alloc = getMinEFTVM(task, solution, subDeadline);
    			
    			VM vm = alloc.getVM();
    			while(alloc.getFinishTime() > subDeadline + Evaluate.E && vm.getType() < VM.FASTEST) {
    				solution.updateVM(vm);
    				alloc.setStartTime(solution.calcEST(task, vm));
    				alloc.setFinishTime(solution.calcEST(task, vm) + task.getTaskSize()/vm.getSpeed());
    			}
    			if(alloc.getFinishTime() > subDeadline + Evaluate.E)
    				violationCount ++;
    		}*/
    		//System.out.println("TT:" + alloc.getVM());
    		solution.addTaskToVM(alloc.getVM(), task, alloc.getStartTime(), true);
    		double currentCost = ((task.getTaskSize()/alloc.getVM().getSpeed()+maxOutTime)/VM.INTERVAL) *alloc.getVM().getUnitCost();
    		//double currentCost = increaseCosts.get(task).get(alloc.getVM());
    		actualCost.put(task, currentCost);
    		sumCost +=currentCost;
    		level_subB.put(currentLevel-1.0, subB-sumCost);
    		
    	}
    	return solution;
    }
    
    private void calculateFactors(Task task, Double subB, Solution solution) {
    	double costFactor;
		double timeFactor;
		double tradeOffFactor;
		Map<VM, Double> vmToCost = new HashMap<>();
		Map<VM, Double> vmToCostFactor = new HashMap<>();
		Map<VM, Double> vmToTimeFactor = new HashMap<>();
		Map<VM, Double> vmToTradeOffFactor = new HashMap<>();
		
		List<Integer> usableType = new ArrayList<>();
    	double startTime, finishTime;
    	double maxECT = 0;
    	double minECT = Double.MAX_VALUE;
    	double currentECT;
    	double oldCost, neuCost, increaseCost;
    	double bestCost = Double.MAX_VALUE;
    	
    	double maxOutTime = 0;
		for(Edge e : task.getOutEdges()){
			maxOutTime = Math.max(maxOutTime, e.getDataSize());
		}
		maxOutTime /= VM.NETWORK_SPEED;
    	
		
		//构建可用vm的type集合，用于生成更高type的vm
		for(VM vm : solution.keySet()) {
			if(!usableType.contains(vm.getType()))
				usableType.add(vm.getType());
		}
		
		//首次循环得到最大、最小值
		for(VM vm : solution.keySet()) {
			//System.out.println("CurrentVM: "+ vm);
			startTime = solution.calcEST(task, vm);
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			if(minECT > (finishTime - startTime))
				minECT = (finishTime - startTime);
			if(maxECT < (finishTime - startTime))
				maxECT = (finishTime - startTime);
			oldCost = Math.ceil(startTime/VM.INTERVAL)*VM.UNIT_COSTS[vm.getType()];
    		neuCost = Math.ceil((finishTime + maxOutTime)/VM.INTERVAL) * VM.UNIT_COSTS[vm.getType()];
    		if(bestCost > (neuCost - oldCost)) 
    			bestCost = (neuCost - oldCost);
		}
		//System.out.println("BestCost is :" + bestCost);
		
		//第二次循环得到每个虚拟机的Factor
		for(VM vm : solution.keySet()) {
			startTime = solution.calcEST(task, vm);
    		finishTime = startTime + task.getTaskSize()/vm.getSpeed();
    		currentECT = finishTime -startTime;
    		oldCost = Math.ceil(startTime/VM.INTERVAL)/VM.UNIT_COSTS[vm.getType()];
    		neuCost = Math.ceil((finishTime + maxOutTime)/VM.INTERVAL) * VM.UNIT_COSTS[vm.getType()];
    		increaseCost = neuCost - oldCost;
    		//System.out.println("Increase: "+ increaseCost);
    		vmToCost.put(vm, increaseCost);
    		
    		if(subB == 0) {
    			vmToCostFactor.put(vm, 0.0);
    			vmToTradeOffFactor.put(vm, 0.0);
    		}else {
    			costFactor = (subB - increaseCost)/(subB - bestCost);
    			//System.out.println("SubB: " + subB);
    			//System.out.println("CostF: " + costFactor);
    			vmToCostFactor.put(vm, costFactor);
    			
    			timeFactor = (maxECT - currentECT)/(maxECT - minECT);
    			if(maxECT - currentECT ==0.0)
    				timeFactor =0.0;
	    		vmToTimeFactor.put(vm, timeFactor);
	    		//System.out.println("MaxECT: " + maxECT);
	    		//System.out.println("CurrentECT: " + currentECT);
	    		//System.out.println("MinECT: " + minECT);
	    		tradeOffFactor = timeFactor/costFactor;
	    		//System.out.println("TOF is calculated to :" + tradeOffFactor);
	    		vmToTradeOffFactor.put(vm, tradeOffFactor);
    		}
    		costFactors.put(task, vmToCostFactor);
    		timeFactors.put(task, vmToTimeFactor);
    		tradeOffFactors.put(task, vmToTradeOffFactor);
		}
    }
    
    private Allocation getBalanceVM(Task task, Solution solution) {
    	VM selectedVM = null;
    	double selectedStartTime = 0;
    	double tradeOffFactor;
    	
    	double maxFactor = 0;
    	double minFactor = Double.MAX_VALUE;
    	double differ;
    	
    	Map<VM, Double> tradeOff = tradeOffFactors.get(task);
    	
    	//选择vm和startTime
    	for(VM vm : solution.keySet()) {
    		tradeOffFactor = tradeOff.get(vm);
    		//System.out.println("TOF: " + tradeOffFactor);
    		//第一种：选择TradeOffFactor最大的
    		/*if(tradeOffFactor > maxFactor) {
    			maxFactor = tradeOffFactor;
    			selectedVM = vm;
    			selectedStartTime = solution.calcEST(task, selectedVM);
    		}*/
    		
    		//第二种：选择TradeOffFactor最接近1的
    		differ = Math.abs(1-tradeOffFactor);
    		//System.out.println("Diff:" + differ);
    		if(differ < minFactor) {
    			minFactor = differ;
    			selectedVM = vm;
    			selectedStartTime = solution.calcEST(task, selectedVM);
    		}
    		//System.out.println("Selected: " + selectedVM);
    		
    		//第三种：选择TradeOffFacotr最小的
    		/*if(tradeOffFactor < minFactor) {
    			minFactor = tradeOffFactor;
    			selectedVM = vm;
    			selectedStartTime = solution.calcEST(task, selectedVM);
    		}*/
    		
    		//初步选择后，根据subDeadline进行判断修正
    		
    	}
    	return new Allocation(selectedVM, task, selectedStartTime);
    }

	 
	private Allocation allocateTask(Task task, Solution solution, Double proSubDeadline, int taskIndex) {
		Allocation alloc = null;
		
		double allocatedSum=0.0;
    	double unallocatedSum=0.0;
		
		for(Task proceed : allocated) {
			allocatedSum += actualCost.get(proceed); 
		}
		for(Task procedent : unallocated) {
			unallocatedSum +=leastCosts.get(procedent);
		}
		VM vm = alloc.getVM();
		while(alloc.getFinishTime() > proSubDeadline + Evaluate.E && vm.getType() < VM.FASTEST){//当前最快的都超过子截止时间，且不是最快等级的，则升级
		    solution.updateVM(vm);			//upgrade������������ĸ��£����ӶȽ�����̫�ࡣ
		    alloc.setStartTime(solution.calcEST(task, vm));
		    alloc.setFinishTime(solution.calcEST(task, vm) + task.getTaskSize()/vm.getSpeed());
	}
		return alloc;
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
		for(Edge e : task.getOutEdges())//OutEdges:从task出发连向其他任务的边
			maxOutTime = Math.max(maxOutTime, e.getDataSize());//遍历取得最大的出边上的数据大小
		maxOutTime /= VM.NETWORK_SPEED;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that meets sub-deadline and minimizes the cost
		if(solution.size()!=0) {
		    for(VM vm : solution.keySet()){
			    startTime = solution.calcEST(task, vm); 
			    finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			    if(finishTime > subDeadline + Evaluate.E)   //sub-deadline not met
			  	    continue;
			    //newVMPeriod 租用时长
			    double newVMPeriod = finishTime + maxOutTime - solution.getVMLeaseStartTime(vm);//finishTime + maxOutTime==LFT(Lease finish Time) （9）
			    double newVMTotalCost = Math.ceil(newVMPeriod/VM.INTERVAL) * vm.getUnitCost();//EC
			    double increasedCost = newVMTotalCost - solution.calcVMCost(vm);  // oldVMTotalCost //后卫原本solution中这个vm上的租用花费
			    //通过对vm的迭代，获得使得总租用花费增长的虚拟机
			    if(increasedCost < minIncreasedCost){ 
				    minIncreasedCost = increasedCost;
				    selectedVM = vm;
				    selectedStartTime = startTime;
			    }
		    }
		}

		//test whether a new VM can meet the sub-deadline and (or) reduce increasedCost; if so, add this new VM
		int selectedI = -1;				
		startTime = solution.calcEST(task, null);//根据是否是第一个任务，决定开始时间
		for(int k = 0 ; k<VM.TYPE_NO; k++){
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[k];//在第k个虚拟机上的结束时间
			if(finishTime > subDeadline + Evaluate.E)	//sub-deadline not met
				continue;
			
			double increasedCost = Math.ceil((finishTime - startTime)/VM.INTERVAL) * VM.UNIT_COSTS[k];
			if(increasedCost < minIncreasedCost){//若有比之前选择的虚拟机增长费用还少的，则给到该虚拟机
				minIncreasedCost = increasedCost;
				selectedI = k;
				selectedStartTime = startTime;
			}
		}
		//若找到了，则付给该vm
		if(selectedI != -1)
			selectedVM = new VM(selectedI);
		//若没有则返回null
		if(selectedVM == null)
			return null;
		else//只要不是null，就返回调度Allocation
			return new Allocation(selectedVM, task, selectedStartTime);
	}
	
	

}
