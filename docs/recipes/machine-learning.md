# Machine Learning Workflows

Practical examples for ML workflows with Molq.

## Model Training Pipeline

```python
from molq import submit

cluster = submit('ml_cluster', 'slurm')

@cluster
def train_model(dataset: str, model_type: str):
    # Preprocess data
    prep_job = yield {
        'cmd': ['python', 'preprocess.py', dataset],
        'cpus': 4, 'memory': '16GB', 'time': '01:00:00'
    }

    # Train model
    train_job = yield {
        'cmd': ['python', 'train.py', model_type],
        'cpus': 16, 'memory': '64GB', 'time': '08:00:00',
        'gpus': 1
    }

    return train_job
```

## Hyperparameter Tuning

```python
@cluster
def hyperparameter_search(param_grid: list):
    jobs = []
    for params in param_grid:
        job = yield {
            'cmd': ['python', 'train.py', '--params', str(params)],
            'cpus': 8, 'memory': '32GB', 'time': '04:00:00'
        }
        jobs.append(job)
    return jobs

# Usage
param_combinations = [
    {'lr': 0.01, 'batch_size': 32},
    {'lr': 0.001, 'batch_size': 64},
    # ... more combinations
]
search_jobs = hyperparameter_search(param_combinations)
```

## Distributed Training

```python
@cluster
def distributed_training(nodes: int):
    return yield {
        'cmd': ['python', '-m', 'torch.distributed.launch',
               '--nproc_per_node=4', 'train_distributed.py'],
        'nodes': nodes,
        'cpus': 16,
        'memory': '64GB',
        'time': '12:00:00',
        'gpus': 4
    }
```

## Model Evaluation

```python
@cluster
def evaluate_models(model_paths: list):
    eval_jobs = []
    for model_path in model_paths:
        job = yield {
            'cmd': ['python', 'evaluate.py', model_path],
            'cpus': 4, 'memory': '8GB', 'time': '00:30:00'
        }
        eval_jobs.append(job)
    return eval_jobs
```

These patterns cover most ML workflows while keeping code concise and readable.
    eval_id = yield {
        'cmd': ['python', 'evaluate.py', 'trained_model.pkl', 'features.pkl'],
        'job_name': 'model_evaluation',
        'cpus': 4,
        'memory': '16GB',
        'time': '00:30:00'
    }

    return {
        'preprocessing': prep_id,
        'features': features_id,
        'training': train_id,
        'evaluation': eval_id
    }

# Usage
model_config = {
    'algorithm': 'random_forest',
    'n_estimators': 100,
    'max_depth': 10,
    'use_gpu': False
}

results = train_classification_model('data/dataset.csv', model_config)
```

### Hyperparameter Optimization

```python
@cluster
def hyperparameter_search(dataset_path: str, param_grid: dict):
    """Perform hyperparameter optimization using grid search."""

    # Prepare data for hyperparameter search
    data_prep_id = yield {
        'cmd': ['python', 'prepare_for_hpo.py', dataset_path],
        'job_name': 'hpo_data_prep',
        'cpus': 4,
        'memory': '16GB',
        'time': '00:30:00'
    }

    # Generate parameter combinations
    from itertools import product

    param_combinations = []
    param_names = list(param_grid.keys())
    param_values = list(param_grid.values())

    for combination in product(*param_values):
        param_dict = dict(zip(param_names, combination))
        param_combinations.append(param_dict)

    # Submit training jobs for each parameter combination
    hpo_jobs = []
    for i, params in enumerate(param_combinations):
        job_id = yield {
            'cmd': ['python', 'train_with_params.py',
                   '--params', json.dumps(params),
                   '--output', f'model_{i}.pkl',
                   '--metrics', f'metrics_{i}.json'],
            'job_name': f'hpo_train_{i}',
            'cpus': 8,
            'memory': '32GB',
            'time': '02:00:00',
            'dependency': data_prep_id
        }
        hpo_jobs.append(job_id)

    # Collect and analyze results
    analysis_id = yield {
        'cmd': ['python', 'analyze_hpo_results.py',
               '--num-models', str(len(param_combinations)),
               '--output', 'best_model.pkl'],
        'job_name': 'hpo_analysis',
        'cpus': 4,
        'memory': '16GB',
        'time': '00:30:00',
        'dependency': hpo_jobs
    }

    return {
        'data_prep': data_prep_id,
        'hpo_jobs': hpo_jobs,
        'analysis': analysis_id,
        'num_combinations': len(param_combinations)
    }

# Usage
param_grid = {
    'n_estimators': [50, 100, 200],
    'max_depth': [5, 10, 15, None],
    'min_samples_split': [2, 5, 10]
}

hpo_results = hyperparameter_search('data/training_set.csv', param_grid)
```

## Deep Learning Workflows

### Distributed Training

```python
@cluster
def distributed_training(model_script: str, dataset_path: str, num_gpus: int = 4):
    """Train a deep learning model using distributed training."""

    # Data preparation for distributed training
    data_id = yield {
        'cmd': ['python', 'prepare_distributed_data.py',
               dataset_path,
               '--num-shards', str(num_gpus)],
        'job_name': 'distributed_data_prep',
        'cpus': 8,
        'memory': '32GB',
        'time': '01:00:00'
    }

    # Distributed training job
    train_id = yield {
        'cmd': ['python', '-m', 'torch.distributed.launch',
               '--nproc_per_node', str(num_gpus),
               model_script,
               '--data-path', dataset_path,
               '--distributed'],
        'job_name': 'distributed_training',
        'nodes': 1,
        'ntasks_per_node': num_gpus,
        'cpus_per_task': 4,
        'memory': '128GB',
        'gpus': num_gpus,
        'time': '24:00:00',
        'partition': 'gpu',
        'dependency': data_id
    }

    return train_id

# Usage
job_id = distributed_training('train_bert.py', 'data/large_corpus.txt', num_gpus=8)
```

### Model Ensemble Training

```python
@cluster
def train_ensemble(dataset_path: str, model_configs: list):
    """Train an ensemble of different models."""

    # Shared data preprocessing
    prep_id = yield {
        'cmd': ['python', 'preprocess_for_ensemble.py', dataset_path],
        'job_name': 'ensemble_data_prep',
        'cpus': 8,
        'memory': '32GB',
        'time': '01:00:00'
    }

    # Train individual models
    model_jobs = []
    for i, config in enumerate(model_configs):
        job_id = yield {
            'cmd': ['python', 'train_model.py',
                   '--config', json.dumps(config),
                   '--data', 'preprocessed_ensemble_data.pkl',
                   '--output', f'ensemble_model_{i}.pkl'],
            'job_name': f'ensemble_model_{i}_{config["type"]}',
            'cpus': config.get('cpus', 16),
            'memory': config.get('memory', '64GB'),
            'gpus': config.get('gpus', 0),
            'time': config.get('time', '08:00:00'),
            'dependency': prep_id
        }
        model_jobs.append(job_id)

    # Combine models into ensemble
    ensemble_id = yield {
        'cmd': ['python', 'create_ensemble.py',
               '--num-models', str(len(model_configs)),
               '--output', 'final_ensemble.pkl'],
        'job_name': 'create_ensemble',
        'cpus': 8,
        'memory': '32GB',
        'time': '01:00:00',
        'dependency': model_jobs
    }

    # Evaluate ensemble
    eval_id = yield {
        'cmd': ['python', 'evaluate_ensemble.py',
               'final_ensemble.pkl',
               'preprocessed_ensemble_data.pkl'],
        'job_name': 'evaluate_ensemble',
        'cpus': 4,
        'memory': '16GB',
        'time': '00:30:00',
        'dependency': ensemble_id
    }

    return {
        'preprocessing': prep_id,
        'individual_models': model_jobs,
        'ensemble': ensemble_id,
        'evaluation': eval_id
    }

# Usage
model_configs = [
    {'type': 'random_forest', 'cpus': 16, 'memory': '32GB', 'time': '04:00:00'},
    {'type': 'xgboost', 'cpus': 16, 'memory': '32GB', 'time': '04:00:00'},
    {'type': 'neural_network', 'cpus': 8, 'memory': '64GB', 'gpus': 1, 'time': '12:00:00'},
    {'type': 'svm', 'cpus': 32, 'memory': '64GB', 'time': '08:00:00'}
]

ensemble_results = train_ensemble('data/training_data.csv', model_configs)
```

## Cross-Validation and Model Selection

### K-Fold Cross-Validation

```python
@cluster
def k_fold_cross_validation(dataset_path: str, model_config: dict, k: int = 5):
    """Perform k-fold cross-validation."""

    # Create k-fold splits
    splits_id = yield {
        'cmd': ['python', 'create_kfold_splits.py',
               dataset_path,
               '--k', str(k),
               '--output-dir', 'cv_splits'],
        'job_name': 'create_cv_splits',
        'cpus': 2,
        'memory': '8GB',
        'time': '00:30:00'
    }

    # Train and evaluate on each fold
    fold_jobs = []
    for fold in range(k):
        job_id = yield {
            'cmd': ['python', 'train_and_evaluate_fold.py',
                   '--train-data', f'cv_splits/fold_{fold}_train.pkl',
                   '--val-data', f'cv_splits/fold_{fold}_val.pkl',
                   '--config', json.dumps(model_config),
                   '--output', f'fold_{fold}_results.json'],
            'job_name': f'cv_fold_{fold}',
            'cpus': 8,
            'memory': '32GB',
            'time': '04:00:00',
            'dependency': splits_id
        }
        fold_jobs.append(job_id)

    # Aggregate cross-validation results
    aggregate_id = yield {
        'cmd': ['python', 'aggregate_cv_results.py',
               '--k', str(k),
               '--output', 'cv_summary.json'],
        'job_name': 'aggregate_cv_results',
        'cpus': 2,
        'memory': '8GB',
        'time': '00:15:00',
        'dependency': fold_jobs
    }

    return {
        'splits': splits_id,
        'folds': fold_jobs,
        'summary': aggregate_id
    }

# Usage
model_config = {
    'algorithm': 'gradient_boosting',
    'n_estimators': 100,
    'learning_rate': 0.1,
    'max_depth': 6
}

cv_results = k_fold_cross_validation('data/dataset.csv', model_config, k=10)
```

### Nested Cross-Validation

```python
@cluster
def nested_cross_validation(dataset_path: str, param_grids: dict, outer_k: int = 5, inner_k: int = 3):
    """Perform nested cross-validation for unbiased model selection."""

    # Create outer cross-validation splits
    outer_splits_id = yield {
        'cmd': ['python', 'create_nested_cv_splits.py',
               dataset_path,
               '--outer-k', str(outer_k),
               '--inner-k', str(inner_k),
               '--output-dir', 'nested_cv_splits'],
        'job_name': 'create_nested_cv_splits',
        'cpus': 4,
        'memory': '16GB',
        'time': '00:30:00'
    }

    outer_fold_jobs = []

    for outer_fold in range(outer_k):
        # For each outer fold, perform inner cross-validation
        inner_jobs = []

        for algorithm, param_grid in param_grids.items():
            inner_cv_id = yield {
                'cmd': ['python', 'inner_cv.py',
                       '--train-data', f'nested_cv_splits/outer_{outer_fold}_train.pkl',
                       '--algorithm', algorithm,
                       '--param-grid', json.dumps(param_grid),
                       '--inner-k', str(inner_k),
                       '--output', f'outer_{outer_fold}_{algorithm}_best_params.json'],
                'job_name': f'inner_cv_outer_{outer_fold}_{algorithm}',
                'cpus': 16,
                'memory': '64GB',
                'time': '08:00:00',
                'dependency': outer_splits_id
            }
            inner_jobs.append(inner_cv_id)

        # Train final model with best parameters and evaluate on outer test set
        outer_eval_id = yield {
            'cmd': ['python', 'outer_evaluation.py',
                   '--train-data', f'nested_cv_splits/outer_{outer_fold}_train.pkl',
                   '--test-data', f'nested_cv_splits/outer_{outer_fold}_test.pkl',
                   '--param-files', ' '.join([f'outer_{outer_fold}_{alg}_best_params.json'
                                            for alg in param_grids.keys()]),
                   '--output', f'outer_fold_{outer_fold}_results.json'],
            'job_name': f'outer_evaluation_{outer_fold}',
            'cpus': 8,
            'memory': '32GB',
            'time': '02:00:00',
            'dependency': inner_jobs
        }
        outer_fold_jobs.append(outer_eval_id)

    # Final aggregation of nested CV results
    final_results_id = yield {
        'cmd': ['python', 'aggregate_nested_cv.py',
               '--outer-k', str(outer_k),
               '--output', 'nested_cv_final_results.json'],
        'job_name': 'aggregate_nested_cv',
        'cpus': 2,
        'memory': '8GB',
        'time': '00:15:00',
        'dependency': outer_fold_jobs
    }

    return {
        'outer_splits': outer_splits_id,
        'outer_evaluations': outer_fold_jobs,
        'final_results': final_results_id
    }

# Usage
param_grids = {
    'random_forest': {
        'n_estimators': [50, 100, 200],
        'max_depth': [5, 10, None],
        'min_samples_split': [2, 5, 10]
    },
    'gradient_boosting': {
        'n_estimators': [50, 100, 200],
        'learning_rate': [0.01, 0.1, 0.2],
        'max_depth': [3, 6, 9]
    }
}

nested_cv_results = nested_cross_validation('data/dataset.csv', param_grids)
```

## Feature Engineering and Selection

### Automated Feature Engineering

```python
@cluster
def automated_feature_engineering(dataset_path: str, target_column: str):
    """Perform automated feature engineering and selection."""

    # Basic feature engineering
    basic_features_id = yield {
        'cmd': ['python', 'basic_feature_engineering.py',
               dataset_path,
               '--target', target_column,
               '--output', 'basic_features.pkl'],
        'job_name': 'basic_feature_engineering',
        'cpus': 8,
        'memory': '32GB',
        'time': '02:00:00'
    }

    # Advanced feature generation (polynomial, interactions, etc.)
    advanced_features_id = yield {
        'cmd': ['python', 'advanced_feature_engineering.py',
               'basic_features.pkl',
               '--output', 'advanced_features.pkl'],
        'job_name': 'advanced_feature_engineering',
        'cpus': 16,
        'memory': '64GB',
        'time': '04:00:00',
        'dependency': basic_features_id
    }

    # Feature selection using multiple methods
    selection_methods = ['univariate', 'rfe', 'lasso', 'tree_importance']
    selection_jobs = []

    for method in selection_methods:
        job_id = yield {
            'cmd': ['python', 'feature_selection.py',
                   'advanced_features.pkl',
                   '--method', method,
                   '--target', target_column,
                   '--output', f'selected_features_{method}.pkl'],
            'job_name': f'feature_selection_{method}',
            'cpus': 8,
            'memory': '32GB',
            'time': '02:00:00',
            'dependency': advanced_features_id
        }
        selection_jobs.append(job_id)

    # Combine and rank features from different selection methods
    combination_id = yield {
        'cmd': ['python', 'combine_feature_selections.py',
               '--methods', ' '.join(selection_methods),
               '--output', 'final_features.pkl'],
        'job_name': 'combine_feature_selections',
        'cpus': 4,
        'memory': '16GB',
        'time': '01:00:00',
        'dependency': selection_jobs
    }

    return {
        'basic_features': basic_features_id,
        'advanced_features': advanced_features_id,
        'selection_jobs': selection_jobs,
        'final_features': combination_id
    }

# Usage
feature_results = automated_feature_engineering('data/raw_dataset.csv', 'target')
```

## Model Deployment and Monitoring

### Model Validation Pipeline

```python
@cluster
def model_validation_pipeline(model_path: str, validation_data: str):
    """Comprehensive model validation before deployment."""

    # Statistical validation tests
    stats_validation_id = yield {
        'cmd': ['python', 'statistical_validation.py',
               model_path,
               validation_data,
               '--output', 'stats_validation_report.json'],
        'job_name': 'statistical_validation',
        'cpus': 4,
        'memory': '16GB',
        'time': '01:00:00'
    }

    # Performance benchmarking
    benchmark_id = yield {
        'cmd': ['python', 'performance_benchmark.py',
               model_path,
               validation_data,
               '--output', 'performance_benchmark.json'],
        'job_name': 'performance_benchmark',
        'cpus': 8,
        'memory': '32GB',
        'time': '02:00:00'
    }

    # Bias and fairness analysis
    fairness_id = yield {
        'cmd': ['python', 'fairness_analysis.py',
               model_path,
               validation_data,
               '--output', 'fairness_report.json'],
        'job_name': 'fairness_analysis',
        'cpus': 4,
        'memory': '16GB',
        'time': '01:00:00'
    }

    # Robustness testing
    robustness_id = yield {
        'cmd': ['python', 'robustness_testing.py',
               model_path,
               validation_data,
               '--output', 'robustness_report.json'],
        'job_name': 'robustness_testing',
        'cpus': 8,
        'memory': '32GB',
        'time': '03:00:00'
    }

    # Generate deployment report
    report_id = yield {
        'cmd': ['python', 'generate_deployment_report.py',
               '--validation-reports', 'stats_validation_report.json',
               'performance_benchmark.json',
               'fairness_report.json',
               'robustness_report.json',
               '--output', 'deployment_readiness_report.html'],
        'job_name': 'deployment_report',
        'cpus': 2,
        'memory': '8GB',
        'time': '00:30:00',
        'dependency': [stats_validation_id, benchmark_id, fairness_id, robustness_id]
    }

    return {
        'statistical_validation': stats_validation_id,
        'performance_benchmark': benchmark_id,
        'fairness_analysis': fairness_id,
        'robustness_testing': robustness_id,
        'deployment_report': report_id
    }

# Usage
validation_results = model_validation_pipeline('models/best_model.pkl', 'data/validation_set.csv')
```

### A/B Testing Framework

```python
@cluster
def ab_testing_analysis(control_model: str, treatment_model: str, test_data: str):
    """Analyze A/B test results between two models."""

    # Generate predictions from both models
    control_pred_id = yield {
        'cmd': ['python', 'generate_predictions.py',
               control_model,
               test_data,
               '--output', 'control_predictions.json'],
        'job_name': 'control_predictions',
        'cpus': 4,
        'memory': '16GB',
        'time': '01:00:00'
    }

    treatment_pred_id = yield {
        'cmd': ['python', 'generate_predictions.py',
               treatment_model,
               test_data,
               '--output', 'treatment_predictions.json'],
        'job_name': 'treatment_predictions',
        'cpus': 4,
        'memory': '16GB',
        'time': '01:00:00'
    }

    # Statistical significance testing
    significance_id = yield {
        'cmd': ['python', 'ab_significance_test.py',
               'control_predictions.json',
               'treatment_predictions.json',
               '--output', 'significance_results.json'],
        'job_name': 'significance_testing',
        'cpus': 2,
        'memory': '8GB',
        'time': '00:30:00',
        'dependency': [control_pred_id, treatment_pred_id]
    }

    # Business impact analysis
    impact_id = yield {
        'cmd': ['python', 'business_impact_analysis.py',
               'control_predictions.json',
               'treatment_predictions.json',
               test_data,
               '--output', 'business_impact.json'],
        'job_name': 'business_impact',
        'cpus': 4,
        'memory': '16GB',
        'time': '01:00:00',
        'dependency': [control_pred_id, treatment_pred_id]
    }

    # Generate A/B test report
    report_id = yield {
        'cmd': ['python', 'generate_ab_report.py',
               'significance_results.json',
               'business_impact.json',
               '--output', 'ab_test_report.html'],
        'job_name': 'ab_test_report',
        'cpus': 2,
        'memory': '8GB',
        'time': '00:30:00',
        'dependency': [significance_id, impact_id]
    }

    return {
        'control_predictions': control_pred_id,
        'treatment_predictions': treatment_pred_id,
        'significance_testing': significance_id,
        'business_impact': impact_id,
        'final_report': report_id
    }

# Usage
ab_results = ab_testing_analysis('models/current_model.pkl',
                                'models/new_model.pkl',
                                'data/ab_test_data.csv')
```

## AutoML and Model Search

### Neural Architecture Search

```python
@cluster
def neural_architecture_search(dataset_path: str, search_space: dict, num_trials: int = 100):
    """Perform neural architecture search."""

    # Prepare data for NAS
    data_prep_id = yield {
        'cmd': ['python', 'prepare_nas_data.py', dataset_path],
        'job_name': 'nas_data_prep',
        'cpus': 4,
        'memory': '16GB',
        'time': '00:30:00'
    }

    # Launch parallel architecture evaluation jobs
    nas_jobs = []
    architectures_per_job = max(1, num_trials // 20)  # Batch architectures

    for batch in range(0, num_trials, architectures_per_job):
        job_id = yield {
            'cmd': ['python', 'evaluate_architectures.py',
                   '--search-space', json.dumps(search_space),
                   '--start-trial', str(batch),
                   '--num-trials', str(min(architectures_per_job, num_trials - batch)),
                   '--output', f'nas_results_batch_{batch}.json'],
            'job_name': f'nas_batch_{batch}',
            'cpus': 8,
            'memory': '64GB',
            'gpus': 1,
            'time': '12:00:00',
            'partition': 'gpu',
            'dependency': data_prep_id
        }
        nas_jobs.append(job_id)

    # Analyze NAS results and select best architecture
    analysis_id = yield {
        'cmd': ['python', 'analyze_nas_results.py',
               '--num-batches', str(len(nas_jobs)),
               '--output', 'best_architecture.json'],
        'job_name': 'nas_analysis',
        'cpus': 4,
        'memory': '16GB',
        'time': '01:00:00',
        'dependency': nas_jobs
    }

    # Train final model with best architecture
    final_training_id = yield {
        'cmd': ['python', 'train_final_nas_model.py',
               'best_architecture.json',
               dataset_path,
               '--output', 'nas_final_model.pkl'],
        'job_name': 'nas_final_training',
        'cpus': 16,
        'memory': '128GB',
        'gpus': 2,
        'time': '24:00:00',
        'partition': 'gpu',
        'dependency': analysis_id
    }

    return {
        'data_prep': data_prep_id,
        'nas_jobs': nas_jobs,
        'analysis': analysis_id,
        'final_training': final_training_id
    }

# Usage
search_space = {
    'num_layers': [3, 4, 5, 6],
    'hidden_size': [64, 128, 256, 512],
    'dropout_rate': [0.1, 0.2, 0.3, 0.4],
    'activation': ['relu', 'gelu', 'swish'],
    'optimizer': ['adam', 'adamw', 'sgd']
}

nas_results = neural_architecture_search('data/training_data.csv', search_space, num_trials=200)
```

This machine learning recipes guide provides comprehensive examples for building ML workflows with Molq, from basic training to advanced techniques like neural architecture search and A/B testing. The patterns shown can be adapted for various ML frameworks and use cases.
