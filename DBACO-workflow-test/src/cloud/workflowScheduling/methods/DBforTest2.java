package cloud.workflowScheduling.methods;

import java.util.*;

import cloud.workflowScheduling.*;
import cloud.workflowScheduling.setting.*;

public class DBforTest2 implements Scheduler{
    private final double bestVMSpeed = VM.SPEEDS[VM.FASTEST];
    double deadline;
    double RB;
    double CPLength;
    double BL;
	//public Solution solution = new Solution();
	private double theta = 2;
	private Map<Task, Double> averageCosts;
	private Map<Task, Double> blCosts;
	private Map<Task, Map<Integer, Double>> standardCosts; 
	private Map<Task, Double> actualCost;
	private List<Task> allocated;
	private List<Task> unallocated;
	private List<VM> aType;
	
	public DBforTest2(double theta) {
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
		blCosts = new HashMap<>();
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
		RB = wf.getBudget();
		Benchmarks bench = new Benchmarks(wf);
		BL = (RB - bench.getCheapSchedule().calcCost())/
				(bench.getFastSchedule().calcCost() - bench.getCheapSchedule().calcCost());
		//System.out.println("CHeck:" + BL);
		
		blCosts = new HashMap<>();
		averageCosts = new HashMap<>();
		standardCosts = new HashMap<>();
		actualCost = new HashMap<>();
		allocated = new ArrayList<>();
		unallocated = new ArrayList<>();
		aType = new ArrayList<>();
		
		//System.out.println("##截止期限为：" + deadline);
		//System.out.println("##预算金额为:" + RB);

		
		
		calculateTasksEST(wfTasks);
		calculateCost(wfTasks);

		return allocateTasks(wf, wfTasks, wf.getDeadline());
	}
	//计算工作流中每个任务的最早开始时间EST
    private void calculateTasksEST(List<Task> wfTasks) {
		Task entryTask = wfTasks.get(0);
		entryTask.setAST(0);
		entryTask.setAFT(0);
		entryTask.setAssigned(true);
		
		for(int i=1; i<wfTasks.size(); i++){		// compute EST, EFT, critical parent via Eqs. 1 and 2; skip entry task
			Task task = wfTasks.get(i);

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
		Task exitTask = wfTasks.get(wfTasks.size()-1);	//Note, EST, EFT, critialParent of exitTask have been set above
		exitTask.setAFT(deadline);
		exitTask.setAST(deadline);
	}
    
    //Calculate the AverageCost and StandardCost for each task on VM of different type
    private void calculateCost(List<Task> tasks) {
    	for(Task task : tasks) {
    		double averageCost = 0;
    		double maxOutTime = 0;
    		double minCost = Double.MAX_VALUE;
    		double maxCost = 0;
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
    			minCost = Math.min(minCost, cost);
    			maxCost = Math.max(maxCost, cost);
    		}
      		averageCost /=VM.TYPE_NO;
    		averageCosts.put(task, averageCost);
    		double blCost = minCost + BL * (maxCost - minCost);
    		blCosts.put(task, blCost);
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
			for(Edge e : task.getOutEdges())//OutEdges:从task出发连向其他任务的边
				maxOutTime = Math.max(maxOutTime, e.getDataSize());//遍历取得最大的出边上的数据大小
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
		for(Task procedent : unallocated) {
			unallocatedSum +=blCosts.get(procedent);
		}
		
		SAB = RB - allocatedSum - unallocatedSum;
		
		
		if(SAB >= 0) {
    		CTB = averageCosts.get(task)+ SAB * averageCosts.get(task)/unallocatedSum;
    	}else CTB = averageCosts.get(task);
		
		for(VM vm : solution.keySet()) {
			if(standardCosts.get(task).get(vm.getType())<CTB) {
				aType.add(vm);
			}
		}
		if(!aType.isEmpty()) {
			alloc = getProperVM(task, solution, proSubDeadline, CTB, taskIndex, aType);
			if(alloc == null)
				alloc = getMinEFTVM(task, solution, proSubDeadline, taskIndex);    
		}
		else if(SAB>=0) {
			alloc = getMinEFTVM(task, solution, proSubDeadline, taskIndex);
			
		}else {
			alloc = getMinCostVM(task, solution, proSubDeadline, taskIndex);
			if(alloc == null){//sl为空			//select a vm which allows EFT
				alloc = getMinEFTVM(task, solution, proSubDeadline, taskIndex);//选择该情况下最快的vm
			}
		}
		VM vm = alloc.getVM();
		while(alloc.getFinishTime() > proSubDeadline + Evaluate.E && vm.getType() < VM.FASTEST){//当前最快的都超过子截止时间，且不是最快等级的，则升级
		    solution.updateVM(vm);			//upgrade������������ĸ��£����ӶȽ�����̫�ࡣ
		    alloc.setStartTime(solution.calcEST(task, vm));
		    alloc.setFinishTime(solution.calcEST(task, vm) + task.getTaskSize()/vm.getSpeed());
	}
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
		for(Edge e : task.getOutEdges())//OutEdges:从task出发连向其他任务的边
			maxOutTime = Math.max(maxOutTime, e.getDataSize());//遍历取得最大的出边上的数据大小
		maxOutTime /= VM.NETWORK_SPEED;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that meets sub-deadline and minimizes the cost
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

		//test whether a new VM can meet the sub-deadline and (or) reduce increasedCost; if so, add this new VM
		int selectedI = -1;				
		startTime = taskIndex==1 ? VM.LAUNCH_TIME : solution.calcEST(task, null);//根据是否是第一个任务，决定开始时间
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
			//newVMPeriod 租用时长
			double newVMPeriod = finishTime + maxOutTime - solution.getVMLeaseStartTime(vm);//finishTime + maxOutTime==LFT(Lease finish Time) （9）
			double newVMTotalCost = Math.ceil(newVMPeriod/VM.INTERVAL) * vm.getUnitCost();//EC
			double increasedCost = newVMTotalCost - solution.calcVMCost(vm);  // oldVMTotalCost //后卫原本solution中这个vm上的租用花费
			//通过对vm的迭代，获得使得总租用花费增长的虚拟机
			if(increasedCost < limitCost){ 
				limitCost = increasedCost;
				selectedVM = vm;
				selectedStartTime = startTime;
			}
		}
		int selectedI = -1;				
		startTime = taskIndex==1 ? VM.LAUNCH_TIME : solution.calcEST(task, null);//根据是否是第一个任务，决定开始时间
		for(int k = 0 ; k<VM.TYPE_NO; k++){
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[k];//在第k个虚拟机上的结束时间
			if(finishTime > subDeadline + Evaluate.E)	//sub-deadline not met
				continue;
			
			double increasedCost = Math.ceil((finishTime - startTime)/VM.INTERVAL) * VM.UNIT_COSTS[k];
			if(increasedCost < limitCost){//若有比之前选择的虚拟机增长费用还少的，则给到该虚拟机
				limitCost = increasedCost;
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
