package oleg.sopilnyak.test2;

import lombok.Data;
import oleg.sopilnyak.test2.model.CampaignBusiness;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

/**
 * Task to test campaigns
 */
public class Campaign implements Runnable {
    private final Set<CampaignBusiness> campaigns = new LinkedHashSet<>();
    private final Map<String, CampaignBusiness> campaignsMap = new HashMap<>();
    private String pathToCampaign = "target/classes/test2/campaign.txt";
    private String pathToInput = "target/classes/test2/input.txt";
    private int starvingFactor = 3;

    @Override
    public void run() {
        System.out.println("Campaign task starts");
        loadCampaignConfiguration();
        processSegmentsInput();
    }

    private void processSegmentsInput() {
        System.out.println("Processing Campaign input data file.");
        int processed = 0;
        try (FileReader reader = new FileReader(new File(pathToInput))) {
            BufferedReader bufferedReader = new BufferedReader(reader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                inputSegment(line);
                processed++;
                if (processed % 100 == 0){
                    System.out.println(new Date()+" - Processed "+processed+" lines.");
                }
            }
            System.out.println("Totally Processed "+processed+" lines.");
            printCampainsByPopularity();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void printCampainsByPopularity() throws IOException {
        System.out.println("Saving results.");
        Comparator<CampaignBusiness> comparator = Comparator.comparing(CampaignBusiness::getPopularity);
        File dir = new File(pathToInput).getParentFile();
        PrintWriter writer = new PrintWriter(new FileWriter(new File(dir,"results.txt")));
        campaigns.stream().sorted(comparator.reversed()).forEach(c->writer.println(c));
        writer.close();
    }

    private void inputSegment(String line) {
        String column[] = line.split(" ");
        Map<String, Integer> result = new HashMap<>();
        Arrays.stream(column, 1, column.length)
                .map(c -> Integer.parseInt(c))
                .forEach(seg->{
                    campaigns.stream().filter(c->c.getSegments().contains(seg)).forEach(c ->{
                        String name = c.getName();
                        int counter = result.computeIfAbsent(name, n->0) + 1;
                        result.put(name, counter);
                    });
                });
        if (result.isEmpty()){
            return;
        }
        if (result.size() == 1){
            find(result.keySet().iterator().next()).incrementPopularity();
            return;
        }
        Map<Integer,List<CampaignBusiness>> loyalMap = new HashMap<>();
        result.entrySet().stream().forEach(e->{
            loyalMap.computeIfAbsent(e.getValue(), c -> new ArrayList<>()).add(find(e.getKey()));
        });

        Comparator<CampaignBusiness> comparator = Comparator.comparing(CampaignBusiness::getPopularity);
        if (loyalMap.size() == 1){
            List<CampaignBusiness> camps = loyalMap.get(loyalMap.keySet().iterator().next()).stream().sorted(comparator.reversed()).collect(Collectors.toList());
            updateWinner(camps);
            return;
        }
        Optional<Integer> max = loyalMap.keySet().stream().max(Comparator.comparing(Integer::intValue));
        List<CampaignBusiness> camps = loyalMap.get(max.get()).stream().sorted(comparator.reversed()).collect(Collectors.toList());
        updateWinner(camps);
    }
    private void updateWinner(List<CampaignBusiness> camps){
        CampaignBusiness winner = camps.get(0);
        if (camps.size() == 1){
            winner.incrementPopularity();
            return;
        }
        CampaignBusiness second = camps.get(1);
        if (winner.getPopularity() - starvingFactor > second.getPopularity()){
            second.incrementPopularity();
            System.out.println("Feed startving campaign "+second.getName());
        }else {
            winner.incrementPopularity();
        }
    }
    private CampaignBusiness find(String name){
        return campaignsMap.get(name);
    }


    private void loadCampaignConfiguration() {
        System.out.println("Loading Campaign data file.");
        try (FileReader reader = new FileReader(new File(pathToCampaign))) {
            BufferedReader bufferedReader = new BufferedReader(reader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                putCampaign(line);
            }
            System.out.println("Loaded Campaigns "+campaigns.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void putCampaign(String line) {
        String column[] = line.split(" ");
        CampaignBusiness campaign = new CampaignBusiness();
        campaign.setName(column[0]);
        campaign.getSegments().addAll(
                Arrays.stream(column, 1, column.length).map(c -> Integer.parseInt(c)).collect(Collectors.toSet())
        );
        campaigns.add(campaign);
        campaignsMap.put(column[0], campaign);
    }
}
