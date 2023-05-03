package cloud.workflowScheduling.methods;

import static java.lang.Math.*;
import java.util.*;

import cloud.workflowScheduling.setting.*;

public class DBACOforTest2 implements Scheduler{
	private static final double ALPHA = 1;
	private static final double BETA = 2;
	private static final double EVAP = 0.8;
	private static final int NO_OF_ITE = 50;
	private static final int NO_OF_EPSILON_ITE = (int)(NO_OF_ITE * 0.7);
	private static final int NO_OF_ANTS = 20;
	
	private Map<Task, Double> leastCosts;
	private Map<Task, Double> averageCosts;
	private Map<Task, Map<Integer, Double>> standardCosts; 
	private Map<Task, Double> actualCost;
	private List<Task> allocated;
	private List<Task> unallocated;
	private List<Integer> aType;
	
	private double [][] pheromone;
	private double [] heuristic;
	private Workflow wf;
	private DBforTest2 PDBC3 = new DBforTest2(1.5);
	private double epsilonDeadline;
	
	@Override
    public Solution schedule(Workflow wf) {
    	this.wf = wf;
    	int size = wf.size();
    	
    	leastCosts = new HashMap<>();
    	averageCosts = new HashMap<>();
		standardCosts = new HashMap<>();
		actualCost = new HashMap<>();
		allocated = new ArrayList<>();
		unallocated = new ArrayList<>();
		aType = new ArrayList<>();
    	heuristic = new double[size];
    	pheromone = new double[size][size];
    	for(int i=0;i<size;i++) {
    		for(int j=0;j<size;j++)
    			pheromone[i][j] = 1;
    	}
    	
    	Benchmarks bench = new Benchmarks(wf);
    	double maxMakespan = bench.getCheapSchedule().calcMakespan();
    	double maxBudget = bench.getFastSchedule().calcCost();
    	Ant gbAnt = null;
    	for(int iterIndex = 0; iterIndex<NO_OF_ITE; iterIndex++) {
    		Ant[] ants = new Ant[NO_OF_ANTS];
    		for(Task t : wf)
    			heuristic[t.getId()] = t.getpURank();
    		
    		if(maxMakespan<wf.getDeadline() || iterIndex >= NO_OF_EPSILON_ITE)
    			epsilonDeadline = wf.getDeadline();
    		else 
    			epsilonDeadline = wf.getDeadline() +
    			    (maxMakespan-wf.getDeadline()) * Math.pow((1-(double)iterIndex/NO_OF_EPSILON_ITE), 4);
    		Ant lbAnt = null;
    		for(int antId=0;antId<NO_OF_ANTS;antId++) {
    			ants[antId] = new Ant();
    			ants[antId].constructTheSolution();
    			if(lbAnt == null || ants[antId].asolution.isBetterThan(lbAnt.asolution, epsilonDeadline))
    				lbAnt = ants[antId];
    		}
    		
    		//update pheromone
    		for(int j=0;j<size;j++)
    			for(int i=0;i<size;i++)
    				pheromone[j][i] *=EVAP;
    		if(gbAnt!=null && random()>0.9)
    			gbAnt.releaseThePheromone();
    		else
    			lbAnt.releaseThePheromone();
    		for(int j=0;j<size;j++) {
    			for(int i=0;i<size;i++) {
    				if(pheromone[j][i]>1)
    					pheromone[j][i]=1;
    				else if(pheromone[j][i]<0.2)
    					pheromone[j][i]=0.2;
    			}
    		}
    		
    		if(gbAnt == null || lbAnt.asolution.isBetterThan(gbAnt.asolution, epsilonDeadline)) {
    			gbAnt = lbAnt;
    			System.out.printf("Iteration index��%3d\t%5.2f\t%5.2f\t%5.2f\n",iterIndex,
						gbAnt.getTheSolution().calcCost(),
						gbAnt.getTheSolution().calcMakespan(),epsilonDeadline);
    		}
    	}
    	
		return gbAnt.getTheSolution();
    }
    
    
    private class Ant {
    	private Solution asolution;
    	private int[] taskIdList = new int[wf.size()];
    	
    	public Ant() {
    		wf.calcPURank(PDBC3.getTheta());
    	}
    	
    	public Solution constructTheSolution() {
    		List<Task> L = new ArrayList<Task>();
    		List<Task> S = new ArrayList<Task>();
    		
    		S.add(wf.get(0));
    		
    		for(Task t : wf)
    			t.setTopoCount(0);
    		
    		int tIndex = 0;
    		while(S.size()>0) {
    			Task task;
    			if(tIndex == 0)
    				task = S.remove(0);
    			else
    				task = chooseNextTask(taskIdList[tIndex], S);
    			
    			taskIdList[tIndex] = task.getId();
    			tIndex++;
    			L.add(task);
    			
    			for(Edge e : task.getOutEdges()) {
    				Task child = e.getDestination();
    				child.setTopoCount(child.getTopoCount()+1);
    				if(child.getTopoCount() == child.getInEdges().size())
    					S.add(child);
    			}
    		}
    		
    		asolution = PDBC3.buildViaTaskList(wf, L, epsilonDeadline);
    		return asolution;
    	}
    	
    	private Task chooseNextTask(int curTaskId, List<Task> S) {
            double sum = 0;		
            for (Task t : S)	
                sum += pow(pheromone[curTaskId][t.getId()], ALPHA) * pow(heuristic[t.getId()], BETA);
            
            double slice = sum * random();
            double k = 0;			
            int chosenIndex = 0;			//the chosen index in S
            for (int indexInS = 0; k < slice; indexInS++) {	
            	Task t = S.get(indexInS);
                k += pow(pheromone[curTaskId][t.getId()], ALPHA) * pow(heuristic[t.getId()], BETA);
                chosenIndex = indexInS;
            }
            return S.remove(chosenIndex);//返回删去的
        }
    	
    	public void releaseThePheromone() {
    		double value = 1 / asolution.calcCost() + 0.5;
    		for(int i = 0;i<taskIdList.length-1; i++)
    			pheromone[taskIdList[i]][taskIdList[i+1]] += value;
    	}
    	
    	public Solution getTheSolution() {
    		return asolution;
    	}
    	
    	@Override 
    	public String toString() {
    		return "Ant [cost=" + asolution.calcCost() +",makespan=" + asolution.calcMakespan() +"]";
    	}
    }
    
    
}
