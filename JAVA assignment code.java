
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;


public class Main {

    // 1. PATIENT CLASS
    static class Patient {
        int id;
        String name;
        String record = "";
        String vitals = "Not recorded";

        Patient(int id, String name) {
            this.id = id;
            this.name = name;
        }

        synchronized void addRecord(String note) {
            record += note + "\n";
        }

        synchronized void updateVitals(String newVitals) {
            vitals = newVitals;
        }

        synchronized void display() {
            System.out.println(name + " | Record: " + record
                    + " | Vitals: " + vitals);
        }
    }

    // 2. SHARED PATIENT REPOSITORY
    static class PatientRepository {
        Map<Integer, Patient> patients = new ConcurrentHashMap<>();

        void add(Patient patient) {
            patients.put(patient.id, patient);
        }

        Patient get(int id) {
            return patients.get(id);
        }
    }

    // 3. DOCTOR CLASS
    static class Doctor {
        String name;
        PatientRepository repository;

        Doctor(String name, PatientRepository repository) {
            this.name = name;
            this.repository = repository;
        }

        void updatePatient(int id) {
            Patient p = repository.get(id);

            if (p != null) {
                p.addRecord(name + " added a diagnosis.");
            }
        }

        void getResults(BlockingQueue<String> results) {
            try {
                for (int i = 0; i < 2; i++) {
                    System.out.println(name + " received: "
                            + results.take());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // 4. NURSE CLASS
    static class Nurse {
        String name;
        PatientRepository repository;
        BlockingQueue<LabRequest> requests;

        Nurse(String name, PatientRepository repository,
                BlockingQueue<LabRequest> requests) {
            this.name = name;
            this.repository = repository;
            this.requests = requests;
        }

        void work(int patientId) {
            Patient p = repository.get(patientId);

            if (p != null) {
                p.updateVitals("BP 120/80, Temperature 37C");

                try {
                    requests.put(
                            new LabRequest(patientId, "Blood Test"));

                    System.out.println(name +
                            " submitted a test for patient " + patientId);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    // 5. LABORATORY REQUEST CLASS
    static class LabRequest {
        int patientId;
        String test;

        LabRequest(int patientId, String test) {
            this.patientId = patientId;
            this.test = test;
        }

        static LabRequest stopSignal() {
            return new LabRequest(-1, "STOP");
        }
    }

    // 6. LABORATORY TECHNICIAN CLASS
    static class LaboratoryTechnician {
        String name;
        BlockingQueue<LabRequest> requests;
        BlockingQueue<String> results;
        Semaphore equipment;

        LaboratoryTechnician(String name,
                BlockingQueue<LabRequest> requests,
                BlockingQueue<String> results,
                Semaphore equipment) {

            this.name = name;
            this.requests = requests;
            this.results = results;
            this.equipment = equipment;
        }

        void processRequests() {
            try {
                while (true) {
                    LabRequest request = requests.take();

                    if (request.patientId == -1) {
                        break;
                    }

                    equipment.acquire();

                    try {
                        System.out.println(name + " processed "
                                + request.test + " for patient "
                                + request.patientId);

                        results.put("Patient " + request.patientId
                                + ": Blood test normal");

                    } finally {
                        equipment.release();
                    }
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // 7. MEDICINE INVENTORY CLASS
    static class MedicineInventory {
        private int doses = 50;
        private final ReentrantLock lock = new ReentrantLock();

        boolean dispense(int quantity) {
            lock.lock();

            try {
                if (quantity <= 0 || doses < quantity) {
                    return false;
                }

                doses -= quantity;
                return true;

            } finally {
                lock.unlock();
            }
        }

        int getDoses() {
            lock.lock();

            try {
                return doses;
            } finally {
                lock.unlock();
            }
        }
    }

    // 8. PHARMACIST CLASS
    static class Pharmacist {
        String name;
        MedicineInventory inventory;

        Pharmacist(String name, MedicineInventory inventory) {
            this.name = name;
            this.inventory = inventory;
        }

        void dispenseMany(int times) {
            for (int i = 0; i < times; i++) {
                if (inventory.dispense(1)) {
                    System.out.println(name + " dispensed one dose.");
                }
            }
        }
    }

    // WAIT FOR THREADS TO FINISH
    static void waitFor(List<Future<?>> tasks) throws Exception {
        for (Future<?> task : tasks) {
            task.get();
        }
    }

    // MAIN METHOD
    public static void main(String[] args) throws Exception {

        // Create shared resources
        PatientRepository repository = new PatientRepository();
        MedicineInventory inventory = new MedicineInventory();

        BlockingQueue<LabRequest> requests = new LinkedBlockingQueue<>(100);

        BlockingQueue<String> results = new LinkedBlockingQueue<>(100);

        Semaphore equipment = new Semaphore(2, true);

        ExecutorService pool = Executors.newFixedThreadPool(20);

        // Register 100 patients
        for (int i = 1; i <= 100; i++) {
            repository.add(new Patient(i, "Patient " + i));
        }

        // Create doctors, nurses and pharmacists
        List<Future<?>> workerTasks = new ArrayList<>();
        Doctor[] doctors = new Doctor[5];

        // 5 DOCTORS
        for (int i = 0; i < 5; i++) {
            doctors[i] = new Doctor("Doctor " + (i + 1), repository);
            Doctor doctor = doctors[i];

            workerTasks.add(pool.submit(() -> doctor.updatePatient(1)));
        }

        // 10 NURSES
        for (int i = 1; i <= 10; i++) {
            Nurse nurse = new Nurse(
                    "Nurse " + i, repository, requests);

            int patientId = i;

            workerTasks.add(pool.submit(() -> nurse.work(patientId)));
        }

        // 2 PHARMACISTS
        for (int i = 1; i <= 2; i++) {
            Pharmacist pharmacist = new Pharmacist(
                    "Pharmacist " + i, inventory);

            workerTasks.add(pool.submit(() -> pharmacist.dispenseMany(30)));
        }

        // Wait for doctors, nurses and pharmacists
        waitFor(workerTasks);

        // 3 LABORATORY TECHNICIANS
        List<Future<?>> labTasks = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            LaboratoryTechnician technician = new LaboratoryTechnician(
                    "Technician " + i,
                    requests, results, equipment);

            labTasks.add(pool.submit(() -> technician.processRequests()));
        }

        // Stop all three technicians after requests are processed
        for (int i = 0; i < 3; i++) {
            requests.put(LabRequest.stopSignal());
        }

        waitFor(labTasks);

        // DOCTORS RETRIEVE LABORATORY RESULTS
        List<Future<?>> doctorTasks = new ArrayList<>();

        for (Doctor doctor : doctors) {
            doctorTasks.add(pool.submit(() -> doctor.getResults(results)));
        }

        waitFor(doctorTasks);

        // Display final results
        repository.get(1).display();

        System.out.println("Final medicine doses: "
                + inventory.getDoses());

        // Stop the ExecutorService
        pool.shutdown();
    }
}

/*
 * G.) Questions
 * a.)
 * 
 */