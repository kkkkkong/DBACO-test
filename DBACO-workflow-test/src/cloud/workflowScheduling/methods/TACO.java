package cloud.workflowScheduling.methods;

//ʱ��Լ�� ��Ⱥ T��Ⱥ
import static java.lang.Math.*;
import java.util.*;

import cloud.workflowScheduling.Evaluate;
import cloud.workflowScheduling.setting.*;

public class TACO implements Scheduler {
	private static final double ALPHA = 1;
	private static final double BETA = 2;
	private static final double EVAP= 0.8;
	private static final int NO_OF_ITE = 50;//迭代次数
	private static final int NO_OF_EPSILON_ITE = (int)(NO_OF_ITE*0.7);//具有时间松弛的迭代次数kT
	private static final int NO_OF_ANTS = 20;//蚁群大小
	private static final double RAND = 0.9;//
	
	private double[][] pheromone; 
	private double[][] heuristic;
	
	private List<Task> tasks;
	
	private double epsilonDeadline;
	
	@Override
	public Solution schedule(Workflow wf) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public Solution schedule(List<Task> ltasks,Double subdeadline,Solution lsolution) {
		tasks = ltasks;
		
		int size = tasks.size();
		int vms = VM.TYPE_NO;
		
		
		Solution gsolution = new Solution(lsolution);
	
		
	
		
		heuristic = new double[size][vms];
		pheromone = new double[size][vms];
		
		for(int i =0;i<size;i++)		//��ʼ����Ϣ�ؾ���
			for(int j=0;j<vms;j++)
				pheromone[i][j] = 1.0 / (tasks.get(i).getTaskSize() / VM.SPEEDS[j] * VM.UNIT_COSTS[j]);
		
		epsilonDeadline = subdeadline;
		//System.out.println("全局解决方案:" + lsolution);
		Ant gbAnt = null;	//globalBestAnt
		
		for(int iterIndex = 0; iterIndex< NO_OF_ITE; iterIndex++){	 //iteration index
			
			Ant[] ants = new Ant[NO_OF_ANTS];
			
			//初始化启发因子-------
			for(int i = 0; i < size; i++) {
				
				Task task = tasks.get(i);
				double costmin = task.minCost();
				double minTime = task.minExeTime();
				
				for(int j = 0; j < vms; j++) {
					double exeTime = task.getTaskSize()/VM.SPEEDS[j];
					double tt = 0.0;
					List<Edge> egs = task.getOutEdges();
					for(Edge e : egs) {
						 tt += e.getDataSize() / VM.NETWORK_SPEED;
					}
					double finishTime = exeTime + tt;
					double cost = exeTime * VM.UNIT_COSTS[j];
					if(finishTime <= subdeadline)
						heuristic[i][j] = (subdeadline/finishTime) * (costmin / cost + minTime / exeTime);
					else
						heuristic[i][j] = 0;
				}
			}
			
			Ant lbAnt = null;	//localBestAnt
			for(int antId = 0;antId<NO_OF_ANTS;antId++){//求出最好的本地蚂蚁解
				//Solution init = new Solution(gsolution);
				//System.out.println("初始蚂蚁信息:"+init);
				ants[antId] = new Ant();
				ants[antId].init(gsolution);
				//System.out.println("蚂蚁" + antId+"第"+iterIndex+"次求解前方案");
				//System.out.println(ants[antId].asolution );
				
				
				ants[antId].constructASolution();
				//System.out.println("蚂蚁" + antId+"第"+iterIndex+"次求解完成方案");
				//System.out.println(ants[antId].asolution);
				if(lbAnt==null || ants[antId].asolution.isBetterThan(lbAnt.asolution, epsilonDeadline))
					lbAnt = ants[antId];
			}
			//System.out.println("蚂蚁求解一次是否干扰全局解决方案" + lsolution);
			
			if(gbAnt!=null && random()>0.9)
				gbAnt.releasePheromone();
			else
				lbAnt.releasePheromone();
			for(int j =0;j<size;j++){
				for(int i=0;i<vms;i++){
					if(pheromone[j][i]>1)
						pheromone[j][i]=1;
					else if(pheromone[j][i]<0.2)
						pheromone[j][i]=0.2;
				}
			}
			
			if(gbAnt==null || lbAnt.asolution.isBetterThan(gbAnt.asolution, epsilonDeadline)){
				//System.out.println("����ȫ������");
				gbAnt = lbAnt;
				//System.out.println("���·���" + gbAnt.getSolution());

			}
			//System.out.println("��"+iterIndex+"�ε������");
		}
//		System.out.printf("Iteration index��%3d\t%5.2f\t%5.2f\t%5.2f\n",000000,
//				gbAnt.getSolution().calcCost(),
//				gbAnt.getSolution().calcMakespan(),epsilonDeadline);
		//System.out.println("全局解决方案" + gbAnt.getSolution());
		lsolution = gbAnt.getSolution();
		//System.out.println("结果是否反馈到：" + lsolution);
		return lsolution;
	}
	
    private class Ant {
		private Solution asolution;
		
		
		public Ant(){
			//this.asolution = new Solution(sol);
		}
		
		public void init(Solution sol) {
			asolution = new Solution(sol);
		}
		
		public Solution constructASolution(){
    		for(int i = 0; i< tasks.size(); i++) {
    			Task task = tasks.get(i);
    			Allocation allo = getVM(task,asolution,epsilonDeadline,i);
    			if(allo == null) {
    				//System.out.println("任务" + task.getId() + "未选到合适虚机，调用getMinEFTVM");
    				allo = getMinEFTVM(task, asolution, epsilonDeadline,i);
    			}
    			asolution.addTaskToVM(allo.getVM(), task, allo.getStartTime(), true);
    		}
    		return asolution;
    	}
    	
        public void releasePheromone() {
        	for(int i = 0;i<tasks.size(); i++)
        		for(int j = 0; j < VM.TYPE_NO; j++)
        			pheromone[i][j] = (1 - EVAP) * pheromone[i][j] + 1 / asolution.calcCost() * EVAP;
        }

    	public Solution getSolution() {
			return asolution;
		}

		@Override
		public String toString() {
			return "Ant [cost=" + asolution.calcCost() + ", makespan=" + asolution.calcMakespan()+ "]";
		}
    }


	
	
	// select a vm that meets sub-deadline and minimizes the cost
	//candidate services include all the services that have been used (i.e., R), 
	//			and those that have not been used but can be added any time (one service for each type)
	public Allocation getVM(Task task, Solution solution, double subDeadline, int taskIndex){

		VM selectedVM = null;
		double selectedStartTime = 0.0;
		
		double maxOutTime = 0.0;	//maxTransferOutTime
		for(Edge e : task.getOutEdges())
			maxOutTime = Math.max(maxOutTime, e.getDataSize());
		maxOutTime /= VM.NETWORK_SPEED;
		
		double factor = Double.MIN_VALUE;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that meets sub-deadline and minimizes the cost
		for(VM vm : solution.keySet()){	
			startTime = solution.calcEST(task, vm); 
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			if(finishTime > subDeadline + Evaluate.E)   //sub-deadline not met
				continue;			
			double itfactor = 0.0;//本次迭代中的信息因子
			
			if(random() <= RAND) {
				itfactor = pheromone[taskIndex][vm.getType()] * pow(heuristic[taskIndex][vm.getType()],ALPHA);
			}else {
				double sumFactor = 0.0;//信息素和启发式因子之和
				for(int i = 0; i < VM.TYPE_NO; i++) {
					sumFactor += pheromone[taskIndex][i] * pow(heuristic[taskIndex][i],ALPHA);
				}
				itfactor = pheromone[taskIndex][vm.getType()] * pow(heuristic[taskIndex][vm.getType()],ALPHA) / sumFactor;
			}
			if(itfactor > factor) {
				factor = itfactor;
				selectedVM = vm;
				selectedStartTime = startTime;
			}
		}

		//test whether a new VM can meet the sub-deadline and (or) reduce increasedCost; if so, add this new VM
		int selectedI = -1;				
		startTime = solution.calcEST(task, null);
		for(int k = 0 ; k<VM.TYPE_NO; k++){
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[k];
			if(finishTime > subDeadline + Evaluate.E)	//sub-deadline not met
				continue;
			
			double itfactor = 0.0;//本次迭代中的信息因子
			
			if(random() <= RAND) {
				itfactor = pheromone[taskIndex][k] * pow(heuristic[taskIndex][k],ALPHA);
			}else {
				double sumFactor = 0.0;//��Ϣ�غ�����ʽ����֮��
				for(int i = 0; i < VM.TYPE_NO; i++) {
					sumFactor += pheromone[taskIndex][i] * pow(heuristic[taskIndex][i],ALPHA);
				}
				itfactor = pheromone[taskIndex][k] * pow(heuristic[taskIndex][k],ALPHA) / sumFactor;
			}
			if(itfactor > factor) {
				factor = itfactor;
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
	
	public Allocation getMinEFTVM(Task task, Solution solution, double subDeadline, int taskIndex){
		VM selectedVM = null;				
		double selectedStartTime = 0;
		double minEFT = Double.MAX_VALUE;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that minimizes EFT
		for(VM vm : solution.keySet()){			
			startTime = solution.calcEST(task, vm); 
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			if(finishTime < subDeadline ){
				minEFT = finishTime;
				selectedVM = vm;
				selectedStartTime = startTime;
				//System.out.println(task.getId() + "选择到了虚拟机" + vm.getId());
			}
		}

		// if solution has no VMs 
		if(selectedVM==null ){		// logically, it is equal to "solution.keySet().size()==0"
			startTime = solution.calcEST(task, null);
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[VM.FASTEST];
			if(finishTime < minEFT){
				//System.out.println(task.getId() + "getMinEFT选择最快的虚拟机");
				minEFT = finishTime;
				selectedStartTime = startTime;
				selectedVM = new VM(VM.FASTEST);
			}
		}
		return  new Allocation(selectedVM, task, selectedStartTime);
	}
}
	
