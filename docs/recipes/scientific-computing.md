# Scientific Computing Workflows

Practical examples for scientific computing with Molq.

## Molecular Dynamics

```python
from molq import submit

cluster = submit('hpc_cluster', 'slurm')

@cluster
def run_md_simulation(system_file: str, steps: int):
    # Prepare system
    prep_job = yield {
        'cmd': ['python', 'prepare.py', system_file],
        'cpus': 4, 'memory': '16GB', 'time': '01:00:00'
    }

    # Run simulation
    sim_job = yield {
        'cmd': ['gmx', 'mdrun', '-s', 'system.tpr', '-nsteps', str(steps)],
        'cpus': 32, 'memory': '64GB', 'time': '24:00:00'
    }

    return sim_job
```

## Parameter Sweeps

```python
@cluster
def parameter_sweep(param_values: list):
    jobs = []
    for value in param_values:
        job = yield {
            'cmd': ['python', 'simulate.py', '--param', str(value)],
            'cpus': 8, 'memory': '16GB', 'time': '04:00:00'
        }
        jobs.append(job)
    return jobs

# Usage
temperatures = [300, 310, 320, 330, 340]
sweep_jobs = parameter_sweep(temperatures)
```

## Data Analysis Pipeline

```python
@cluster
def analyze_results(data_files: list):
    # Process individual files
    process_jobs = []
    for file in data_files:
        job = yield {
            'cmd': ['python', 'process.py', file],
            'cpus': 4, 'memory': '8GB', 'time': '02:00:00'
        }
        process_jobs.append(job)

    # Aggregate results
    aggregate_job = yield {
        'cmd': ['python', 'aggregate.py', '--input', 'processed_*.dat'],
        'cpus': 2, 'memory': '16GB', 'time': '01:00:00'
    }

    return aggregate_job
```

## High-Throughput Computing

```python
@cluster
def batch_processing(input_files: list, batch_size: int = 10):
    # Process files in batches
    for i in range(0, len(input_files), batch_size):
        batch = input_files[i:i+batch_size]
        yield {
            'cmd': ['python', 'batch_process.py'] + batch,
            'cpus': 16, 'memory': '32GB', 'time': '08:00:00'
        }
```

These patterns cover common scientific computing workflows while maintaining simplicity.
               '-deffnm', 'nvt_equilibration',
               '-v'],
        'job_name': 'nvt_equilibration',
        'cpus': 32,
        'memory': '64GB',
        'time': '04:00:00',
        'dependency': minimize_id
    }
    equilibration_jobs.append(nvt_id)

    # NPT equilibration
    npt_id = yield {
        'cmd': ['gmx', 'mdrun',
               '-s', 'npt.tpr',
               '-deffnm', 'npt_equilibration',
               '-v'],
        'job_name': 'npt_equilibration',
        'cpus': 32,
        'memory': '64GB',
        'time': '04:00:00',
        'dependency': nvt_id
    }
    equilibration_jobs.append(npt_id)

    # Production run
    production_id = yield {
        'cmd': ['gmx', 'mdrun',
               '-s', 'production.tpr',
               '-deffnm', 'production_run',
               '-v'],
        'job_name': 'md_production',
        'cpus': 64,
        'memory': '128GB',
        'time': '48:00:00',
        'nodes': 2,
        'ntasks_per_node': 32,
        'dependency': npt_id
    }

    # Analysis
    analysis_id = yield {
        'cmd': ['python', 'analyze_trajectory.py',
               'production_run.xtc',
               'prepared_system.pdb',
               '--output', 'analysis_results.json'],
        'job_name': 'trajectory_analysis',
        'cpus': 16,
        'memory': '64GB',
        'time': '04:00:00',
        'dependency': production_id
    }

    return {
        'preparation': prep_id,
        'minimization': minimize_id,
        'equilibration': equilibration_jobs,
        'production': production_id,
        'analysis': analysis_id
    }

# Usage
md_parameters = {
    'temperature': 300,  # K
    'pressure': 1.0,     # bar
    'simulation_time': 100,  # ns
    'timestep': 2.0,     # fs
    'force_field': 'amber99sb-ildn'
}

md_results = molecular_dynamics_simulation('protein_system.pdb', md_parameters)
```

### Enhanced Sampling Methods

```python
@cluster
def umbrella_sampling_simulation(system_file: str, reaction_coordinate: dict):
    """Perform umbrella sampling for free energy calculations."""

    # Prepare umbrella sampling windows
    prep_id = yield {
        'cmd': ['python', 'prepare_umbrella_sampling.py',
               system_file,
               '--coordinate', str(reaction_coordinate),
               '--num-windows', '40',
               '--output-dir', 'umbrella_windows'],
        'job_name': 'umbrella_prep',
        'cpus': 8,
        'memory': '32GB',
        'time': '02:00:00'
    }

    # Run parallel umbrella sampling simulations
    umbrella_jobs = []
    num_windows = 40

    for window in range(num_windows):
        job_id = yield {
            'cmd': ['gmx', 'mdrun',
                   '-s', f'umbrella_windows/window_{window}.tpr',
                   '-deffnm', f'umbrella_window_{window}',
                   '-px', f'pullx_{window}.xvg',
                   '-pf', f'pullf_{window}.xvg'],
            'job_name': f'umbrella_window_{window}',
            'cpus': 16,
            'memory': '32GB',
            'time': '24:00:00',
            'dependency': prep_id
        }
        umbrella_jobs.append(job_id)

    # WHAM analysis for free energy profile
    wham_id = yield {
        'cmd': ['gmx', 'wham',
               '-it', 'tpr_files.dat',
               '-if', 'pullf_files.dat',
               '-o', 'pmf_profile.xvg',
               '-hist', 'umbrella_histograms.xvg'],
        'job_name': 'wham_analysis',
        'cpus': 8,
        'memory': '16GB',
        'time': '02:00:00',
        'dependency': umbrella_jobs
    }

    return {
        'preparation': prep_id,
        'umbrella_simulations': umbrella_jobs,
        'wham_analysis': wham_id
    }

# Usage
reaction_coord = {
    'type': 'distance',
    'group1': 'protein_active_site',
    'group2': 'ligand_binding_group',
    'range': [0.5, 3.0],  # nm
    'force_constant': 1000  # kJ/mol/nm^2
}

umbrella_results = umbrella_sampling_simulation('protein_ligand.pdb', reaction_coord)
```

## Quantum Chemistry Calculations

### Electronic Structure Calculations

```python
@cluster
def quantum_chemistry_workflow(molecule_file: str, calculation_type: str, basis_set: str):
    """Perform quantum chemistry calculations with various methods."""

    # Geometry optimization
    geom_opt_id = yield {
        'cmd': ['orca', f'{molecule_file}_geom_opt.inp'],
        'job_name': 'geometry_optimization',
        'cpus': 16,
        'memory': '64GB',
        'time': '12:00:00'
    }

    # Single point energy calculation
    if calculation_type in ['CCSD(T)', 'MP2']:
        sp_id = yield {
            'cmd': ['orca', f'{molecule_file}_sp_{calculation_type}.inp'],
            'job_name': f'single_point_{calculation_type}',
            'cpus': 32,
            'memory': '128GB',
            'time': '48:00:00',
            'dependency': geom_opt_id
        }
    else:
        sp_id = yield {
            'cmd': ['orca', f'{molecule_file}_sp_{calculation_type}.inp'],
            'job_name': f'single_point_{calculation_type}',
            'cpus': 16,
            'memory': '64GB',
            'time': '08:00:00',
            'dependency': geom_opt_id
        }

    # Frequency calculation
    freq_id = yield {
        'cmd': ['orca', f'{molecule_file}_freq.inp'],
        'job_name': 'frequency_calculation',
        'cpus': 16,
        'memory': '64GB',
        'time': '24:00:00',
        'dependency': geom_opt_id
    }

    # Natural bond orbital analysis
    nbo_id = yield {
        'cmd': ['orca', f'{molecule_file}_nbo.inp'],
        'job_name': 'nbo_analysis',
        'cpus': 8,
        'memory': '32GB',
        'time': '04:00:00',
        'dependency': sp_id
    }

    # Property analysis
    analysis_id = yield {
        'cmd': ['python', 'analyze_qchem_results.py',
               '--geometry', f'{molecule_file}_geom_opt.out',
               '--energy', f'{molecule_file}_sp_{calculation_type}.out',
               '--frequency', f'{molecule_file}_freq.out',
               '--nbo', f'{molecule_file}_nbo.out',
               '--output', 'qchem_analysis.json'],
        'job_name': 'qchem_analysis',
        'cpus': 4,
        'memory': '16GB',
        'time': '01:00:00',
        'dependency': [sp_id, freq_id, nbo_id]
    }

    return {
        'geometry_optimization': geom_opt_id,
        'single_point': sp_id,
        'frequency': freq_id,
        'nbo_analysis': nbo_id,
        'final_analysis': analysis_id
    }

# Usage
qchem_results = quantum_chemistry_workflow('benzene.xyz', 'B3LYP', 'def2-TZVP')
```

### Potential Energy Surface Scanning

```python
@cluster
def pes_scan_workflow(molecule_file: str, scan_parameters: dict):
    """Perform potential energy surface scan."""

    # Generate scan points
    scan_prep_id = yield {
        'cmd': ['python', 'generate_scan_points.py',
               molecule_file,
               '--parameters', str(scan_parameters),
               '--output-dir', 'pes_scan_points'],
        'job_name': 'pes_scan_prep',
        'cpus': 4,
        'memory': '16GB',
        'time': '01:00:00'
    }

    # Run calculations for each scan point
    scan_jobs = []
    num_points = scan_parameters['num_points']

    for point in range(num_points):
        job_id = yield {
            'cmd': ['orca', f'pes_scan_points/point_{point}.inp'],
            'job_name': f'pes_point_{point}',
            'cpus': 16,
            'memory': '64GB',
            'time': '08:00:00',
            'dependency': scan_prep_id
        }
        scan_jobs.append(job_id)

    # Analyze PES and find transition states
    analysis_id = yield {
        'cmd': ['python', 'analyze_pes.py',
               '--scan-dir', 'pes_scan_points',
               '--num-points', str(num_points),
               '--output', 'pes_analysis.json'],
        'job_name': 'pes_analysis',
        'cpus': 8,
        'memory': '32GB',
        'time': '02:00:00',
        'dependency': scan_jobs
    }

    # Transition state optimization
    ts_opt_id = yield {
        'cmd': ['python', 'optimize_transition_states.py',
               'pes_analysis.json',
               '--output-dir', 'transition_states'],
        'job_name': 'ts_optimization',
        'cpus': 16,
        'memory': '64GB',
        'time': '12:00:00',
        'dependency': analysis_id
    }

    return {
        'scan_preparation': scan_prep_id,
        'scan_calculations': scan_jobs,
        'pes_analysis': analysis_id,
        'ts_optimization': ts_opt_id
    }

# Usage
scan_params = {
    'coordinate': 'dihedral',
    'atoms': [1, 2, 3, 4],
    'range': [-180, 180],
    'num_points': 36,
    'step_size': 10
}

pes_results = pes_scan_workflow('reactant.xyz', scan_params)
```

## Climate and Weather Modeling

### Weather Forecast Simulation

```python
@cluster
def weather_forecast_workflow(initial_conditions: str, forecast_hours: int):
    """Run weather forecast simulation using WRF model."""

    # Prepare initial and boundary conditions
    prep_id = yield {
        'cmd': ['python', 'prepare_wrf_input.py',
               initial_conditions,
               '--forecast-hours', str(forecast_hours),
               '--output-dir', 'wrf_input'],
        'job_name': 'wrf_input_prep',
        'cpus': 8,
        'memory': '32GB',
        'time': '02:00:00'
    }

    # Run WPS (WRF Preprocessing System)
    wps_id = yield {
        'cmd': ['./run_wps.sh'],
        'job_name': 'wrf_preprocessing',
        'cpus': 16,
        'memory': '64GB',
        'time': '04:00:00',
        'dependency': prep_id
    }

    # Run WRF simulation
    wrf_id = yield {
        'cmd': ['mpirun', '-np', '64', './wrf.exe'],
        'job_name': 'wrf_simulation',
        'nodes': 4,
        'ntasks_per_node': 16,
        'memory': '256GB',
        'time': '24:00:00',
        'dependency': wps_id
    }

    # Post-processing and visualization
    postproc_id = yield {
        'cmd': ['python', 'postprocess_wrf_output.py',
               'wrfout_*',
               '--output-dir', 'forecast_products'],
        'job_name': 'wrf_postprocessing',
        'cpus': 16,
        'memory': '64GB',
        'time': '04:00:00',
        'dependency': wrf_id
    }

    return {
        'input_preparation': prep_id,
        'preprocessing': wps_id,
        'simulation': wrf_id,
        'postprocessing': postproc_id
    }

# Usage
forecast_results = weather_forecast_workflow('gfs_initial_conditions.grb', 72)
```

### Climate Model Ensemble

```python
@cluster
def climate_ensemble_simulation(base_config: dict, perturbations: list):
    """Run climate model ensemble with parameter perturbations."""

    # Prepare base configuration
    base_prep_id = yield {
        'cmd': ['python', 'prepare_base_climate_config.py',
               '--config', str(base_config),
               '--output', 'base_config.json'],
        'job_name': 'base_config_prep',
        'cpus': 4,
        'memory': '16GB',
        'time': '01:00:00'
    }

    # Run ensemble members
    ensemble_jobs = []

    for i, perturbation in enumerate(perturbations):
        # Prepare perturbed configuration
        config_id = yield {
            'cmd': ['python', 'create_perturbed_config.py',
                   'base_config.json',
                   '--perturbation', str(perturbation),
                   '--member', str(i),
                   '--output', f'ensemble_config_{i}.json'],
            'job_name': f'config_member_{i}',
            'cpus': 2,
            'memory': '8GB',
            'time': '00:30:00',
            'dependency': base_prep_id
        }

        # Run climate simulation
        sim_id = yield {
            'cmd': ['mpirun', '-np', '128', './climate_model.exe',
                   f'ensemble_config_{i}.json'],
            'job_name': f'climate_sim_member_{i}',
            'nodes': 8,
            'ntasks_per_node': 16,
            'memory': '512GB',
            'time': '72:00:00',
            'dependency': config_id
        }

        ensemble_jobs.append(sim_id)

    # Ensemble analysis
    analysis_id = yield {
        'cmd': ['python', 'analyze_ensemble.py',
               '--num-members', str(len(perturbations)),
               '--output', 'ensemble_analysis.json'],
        'job_name': 'ensemble_analysis',
        'cpus': 32,
        'memory': '128GB',
        'time': '08:00:00',
        'dependency': ensemble_jobs
    }

    return {
        'base_preparation': base_prep_id,
        'ensemble_simulations': ensemble_jobs,
        'ensemble_analysis': analysis_id
    }

# Usage
base_climate_config = {
    'model': 'CESM2',
    'resolution': '1deg',
    'simulation_years': 50,
    'scenario': 'SSP245'
}

parameter_perturbations = [
    {'cloud_feedback': +0.1},
    {'cloud_feedback': -0.1},
    {'aerosol_forcing': +0.2},
    {'aerosol_forcing': -0.2},
    {'ocean_mixing': +0.15},
    {'ocean_mixing': -0.15}
]

ensemble_results = climate_ensemble_simulation(base_climate_config, parameter_perturbations)
```

## Computational Fluid Dynamics

### CFD Simulation Pipeline

```python
@cluster
def cfd_simulation_workflow(geometry_file: str, flow_conditions: dict):
    """Complete CFD simulation workflow."""

    # Mesh generation
    mesh_id = yield {
        'cmd': ['python', 'generate_mesh.py',
               geometry_file,
               '--conditions', str(flow_conditions),
               '--output', 'simulation_mesh.msh'],
        'job_name': 'mesh_generation',
        'cpus': 16,
        'memory': '64GB',
        'time': '04:00:00'
    }

    # Mesh quality check
    quality_id = yield {
        'cmd': ['python', 'check_mesh_quality.py',
               'simulation_mesh.msh',
               '--output', 'mesh_quality_report.json'],
        'job_name': 'mesh_quality_check',
        'cpus': 4,
        'memory': '16GB',
        'time': '01:00:00',
        'dependency': mesh_id
    }

    # CFD simulation
    cfd_id = yield {
        'cmd': ['mpirun', '-np', '128', 'openfoam_solver',
               '-case', 'cfd_case',
               '-parallel'],
        'job_name': 'cfd_simulation',
        'nodes': 8,
        'ntasks_per_node': 16,
        'memory': '512GB',
        'time': '48:00:00',
        'dependency': quality_id
    }

    # Post-processing
    postproc_id = yield {
        'cmd': ['python', 'postprocess_cfd.py',
               '--case-dir', 'cfd_case',
               '--output-dir', 'cfd_results'],
        'job_name': 'cfd_postprocessing',
        'cpus': 16,
        'memory': '64GB',
        'time': '04:00:00',
        'dependency': cfd_id
    }

    # Visualization
    viz_id = yield {
        'cmd': ['python', 'create_cfd_visualization.py',
               'cfd_results',
               '--output', 'cfd_visualization.html'],
        'job_name': 'cfd_visualization',
        'cpus': 8,
        'memory': '32GB',
        'time': '02:00:00',
        'dependency': postproc_id
    }

    return {
        'mesh_generation': mesh_id,
        'mesh_quality': quality_id,
        'simulation': cfd_id,
        'postprocessing': postproc_id,
        'visualization': viz_id
    }

# Usage
flow_conditions = {
    'reynolds_number': 100000,
    'mach_number': 0.3,
    'turbulence_model': 'k_omega_sst',
    'inlet_velocity': 50.0,  # m/s
    'outlet_pressure': 101325  # Pa
}

cfd_results = cfd_simulation_workflow('airfoil_geometry.step', flow_conditions)
```

## Astrophysics and Cosmology

### N-Body Simulation

```python
@cluster
def nbody_cosmological_simulation(initial_conditions: str, simulation_params: dict):
    """Run large-scale N-body cosmological simulation."""

    # Generate initial conditions
    ic_id = yield {
        'cmd': ['python', 'generate_cosmological_ics.py',
               '--params', str(simulation_params),
               '--output', 'initial_conditions.dat'],
        'job_name': 'generate_initial_conditions',
        'cpus': 32,
        'memory': '128GB',
        'time': '08:00:00'
    }

    # Run N-body simulation
    nbody_id = yield {
        'cmd': ['mpirun', '-np', '512', './gadget4',
               'cosmological_params.txt'],
        'job_name': 'nbody_simulation',
        'nodes': 32,
        'ntasks_per_node': 16,
        'memory': '2TB',
        'time': '168:00:00',  # 1 week
        'partition': 'large-mem',
        'dependency': ic_id
    }

    # Halo finding
    halo_id = yield {
        'cmd': ['mpirun', '-np', '128', './rockstar',
               'rockstar_config.cfg'],
        'job_name': 'halo_finding',
        'nodes': 8,
        'ntasks_per_node': 16,
        'memory': '512GB',
        'time': '24:00:00',
        'dependency': nbody_id
    }

    # Power spectrum analysis
    power_spectrum_id = yield {
        'cmd': ['python', 'compute_power_spectrum.py',
               'simulation_output/',
               '--output', 'power_spectrum_results.json'],
        'job_name': 'power_spectrum_analysis',
        'cpus': 32,
        'memory': '128GB',
        'time': '08:00:00',
        'dependency': nbody_id
    }

    # Visualization and analysis
    viz_id = yield {
        'cmd': ['python', 'visualize_cosmic_web.py',
               'simulation_output/',
               'halo_catalogs/',
               '--output-dir', 'visualization_products'],
        'job_name': 'cosmic_web_visualization',
        'cpus': 16,
        'memory': '64GB',
        'time': '04:00:00',
        'dependency': [halo_id, power_spectrum_id]
    }

    return {
        'initial_conditions': ic_id,
        'nbody_simulation': nbody_id,
        'halo_finding': halo_id,
        'power_spectrum': power_spectrum_id,
        'visualization': viz_id
    }

# Usage
cosmo_params = {
    'box_size': 1000,  # Mpc/h
    'num_particles': 1024**3,
    'omega_matter': 0.308,
    'omega_lambda': 0.692,
    'hubble_constant': 0.678,
    'sigma_8': 0.815,
    'redshift_start': 99,
    'redshift_end': 0
}

nbody_results = nbody_cosmological_simulation('planck_cosmology.txt', cosmo_params)
```

## High-Energy Physics Analysis

### Particle Physics Data Analysis

```python
@cluster
def hep_analysis_workflow(data_files: list, analysis_config: dict):
    """High-energy physics data analysis workflow."""

    # Data preprocessing and skimming
    skim_jobs = []
    for i, data_file in enumerate(data_files):
        skim_id = yield {
            'cmd': ['python', 'skim_events.py',
                   data_file,
                   '--config', str(analysis_config),
                   '--output', f'skimmed_data_{i}.root'],
            'job_name': f'skim_data_{i}',
            'cpus': 8,
            'memory': '32GB',
            'time': '04:00:00'
        }
        skim_jobs.append(skim_id)

    # Merge skimmed data
    merge_id = yield {
        'cmd': ['hadd', 'merged_skimmed_data.root'] + [f'skimmed_data_{i}.root' for i in range(len(data_files))],
        'job_name': 'merge_skimmed_data',
        'cpus': 4,
        'memory': '16GB',
        'time': '02:00:00',
        'dependency': skim_jobs
    }

    # Event selection and analysis
    analysis_id = yield {
        'cmd': ['python', 'hep_analysis.py',
               'merged_skimmed_data.root',
               '--analysis-type', analysis_config['analysis_type'],
               '--output', 'analysis_results.root'],
        'job_name': 'hep_event_analysis',
        'cpus': 16,
        'memory': '64GB',
        'time': '08:00:00',
        'dependency': merge_id
    }

    # Statistical analysis
    stats_id = yield {
        'cmd': ['python', 'statistical_analysis.py',
               'analysis_results.root',
               '--config', str(analysis_config),
               '--output', 'statistical_results.json'],
        'job_name': 'statistical_analysis',
        'cpus': 8,
        'memory': '32GB',
        'time': '04:00:00',
        'dependency': analysis_id
    }

    # Systematic uncertainties
    systematics_id = yield {
        'cmd': ['python', 'systematic_uncertainties.py',
               'analysis_results.root',
               'statistical_results.json',
               '--output', 'systematic_analysis.json'],
        'job_name': 'systematic_analysis',
        'cpus': 16,
        'memory': '64GB',
        'time': '08:00:00',
        'dependency': stats_id
    }

    # Final plots and results
    plots_id = yield {
        'cmd': ['python', 'create_final_plots.py',
               'statistical_results.json',
               'systematic_analysis.json',
               '--output-dir', 'final_plots'],
        'job_name': 'create_plots',
        'cpus': 4,
        'memory': '16GB',
        'time': '02:00:00',
        'dependency': systematics_id
    }

    return {
        'skimming': skim_jobs,
        'merging': merge_id,
        'analysis': analysis_id,
        'statistics': stats_id,
        'systematics': systematics_id,
        'plots': plots_id
    }

# Usage
hep_data_files = [f'data_sample_{i}.root' for i in range(100)]
hep_config = {
    'analysis_type': 'higgs_to_diphoton',
    'energy_center_of_mass': 13000,  # GeV
    'luminosity': 139.0,  # fb^-1
    'signal_region': {
        'diphoton_mass': [120, 130],  # GeV
        'photon_pt': [25, 25],  # GeV
        'photon_eta': 2.37
    }
}

hep_results = hep_analysis_workflow(hep_data_files, hep_config)
```

## Optimization and Parameter Studies

### Multi-Objective Optimization

```python
@cluster
def multi_objective_optimization(objective_function: str, parameter_space: dict, algorithm: str = 'NSGA-II'):
    """Perform multi-objective optimization using evolutionary algorithms."""

    # Initialize optimization
    init_id = yield {
        'cmd': ['python', 'initialize_optimization.py',
               '--objective-function', objective_function,
               '--parameter-space', str(parameter_space),
               '--algorithm', algorithm,
               '--output', 'optimization_config.json'],
        'job_name': 'optimization_init',
        'cpus': 4,
        'memory': '16GB',
        'time': '01:00:00'
    }

    # Run optimization generations
    generation_jobs = []
    num_generations = 100
    population_size = 200

    for generation in range(num_generations):
        gen_id = yield {
            'cmd': ['python', 'run_optimization_generation.py',
                   'optimization_config.json',
                   '--generation', str(generation),
                   '--population-size', str(population_size),
                   '--output', f'generation_{generation}_results.json'],
            'job_name': f'optimization_gen_{generation}',
            'cpus': 32,
            'memory': '128GB',
            'time': '12:00:00',
            'dependency': init_id if generation == 0 else generation_jobs[-1]
        }
        generation_jobs.append(gen_id)

    # Analyze Pareto front
    pareto_id = yield {
        'cmd': ['python', 'analyze_pareto_front.py',
               '--num-generations', str(num_generations),
               '--output', 'pareto_analysis.json'],
        'job_name': 'pareto_analysis',
        'cpus': 8,
        'memory': '32GB',
        'time': '02:00:00',
        'dependency': generation_jobs
    }

    # Sensitivity analysis
    sensitivity_id = yield {
        'cmd': ['python', 'sensitivity_analysis.py',
               'pareto_analysis.json',
               '--parameter-space', str(parameter_space),
               '--output', 'sensitivity_results.json'],
        'job_name': 'sensitivity_analysis',
        'cpus': 16,
        'memory': '64GB',
        'time': '04:00:00',
        'dependency': pareto_id
    }

    return {
        'initialization': init_id,
        'generations': generation_jobs,
        'pareto_analysis': pareto_id,
        'sensitivity_analysis': sensitivity_id
    }

# Usage
param_space = {
    'design_variable_1': {'type': 'continuous', 'bounds': [0.0, 10.0]},
    'design_variable_2': {'type': 'continuous', 'bounds': [-5.0, 5.0]},
    'design_variable_3': {'type': 'discrete', 'choices': [1, 2, 3, 4, 5]},
    'design_variable_4': {'type': 'categorical', 'choices': ['A', 'B', 'C']}
}

optimization_results = multi_objective_optimization('engineering_objectives.py', param_space, 'NSGA-III')
```

This scientific computing guide provides comprehensive examples for various computational science domains using Molq. The patterns can be adapted for different scientific software packages and research workflows, enabling efficient execution of complex computational tasks on HPC systems.
